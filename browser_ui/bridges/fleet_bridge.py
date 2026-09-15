"""
Fleet launch bridge
Executes and supervises fleet-coordinator's coordinator_node as a subprocess on behalf of argOS's Plan Mission "Launch"
step

coordinator_node's job is to own and drive a live
mission for its full duration -- TF lookups, release-gating, dispatch,
deadlock checks, all continuously until the mission finishes or is killed.
That's a supervised one-shot-per-mission *process*, not a stateless
computation; wrapping it behind a server would just relocate the same
process-lifecycle problem behind another layer; not remove it.

Single-mission slot, matching coordinator_node's own one-mission-per-run
shape -- a second launch is refused (by main.py, before this ever sees it)
while one is already running.

Polls argOS for one thing at a time:
  GET  /mission/launch/command  -- {action: "launch"|"stop"|null, yaml, params}
  POST /mission/launch/status   -- {status, pid, returncode, message}

Standalone (run in the same ROS2-sourced shell fleet-coordinator itself
runs in -- this only ever shells out to `ros2 run`, no rclpy needed here):
    python fleet_bridge.py --cloud-url http://localhost:8080
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import tempfile
import time

import requests

logging.basicConfig(level=logging.INFO, format="[fleet_bridge] %(message)s")
log = logging.getLogger(__name__)

DEFAULT_CLOUD = os.environ.get("CLOUD_URL", "http://localhost:8080")
DEFAULT_POLL_HZ = float(os.environ.get("FLEET_BRIDGE_POLL_HZ", "1"))
BRIDGE_PASSWORD = os.environ.get("BRIDGE_PASSWORD", "")
STOP_GRACE_SECONDS = 5.0


class MissionProcess:
    """Owns at most one coordinator_node subprocess at a time."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.mission_path: str | None = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, yaml_text: str, params: dict) -> tuple[bool, str]:
        if self.running:
            return False, "a mission is already running"

        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", prefix="argos_mission_", delete=False
        )
        fh.write(yaml_text)
        fh.close()
        self.mission_path = fh.name

        cmd = [
            "ros2",
            "run",
            "fleet_coordinator",
            "coordinator_node",
            "--ros-args",
            "-p",
            f"mission_file:={self.mission_path}",
            "-p",
            f"use_sim_time:={'true' if params.get('use_sim_time') else 'false'}",
            "-p",
            f"initial_pose.publish:={'true' if params.get('initial_pose_publish') else 'false'}",
        ]
        map_id = params.get("spooky_map_id")
        if map_id:
            cmd += ["-p", f"spooky.map_id:={map_id}"]

        log.info(f"launching: {' '.join(cmd)}")
        try:
            # New process group -- see module docstring. Without preexec_fn
            # here, stop() below can only ever kill this one PID, not
            # whatever coordinator_node itself spawns underneath it.
            self.proc = subprocess.Popen(cmd, preexec_fn=os.setsid)
        except Exception as e:
            return False, f"failed to launch: {e}"
        return True, f"launched pid={self.proc.pid}"

    def stop(self) -> str:
        if not self.running:
            return "not running"
        pid = self.proc.pid
        try:
            pgid = os.getpgid(pid)
            log.info(f"stopping pid={pid} (pgid={pgid})")
            os.killpg(pgid, signal.SIGTERM)
            self.proc.wait(timeout=STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            log.warning(f"pid={pid} did not exit after SIGTERM, sending SIGKILL")
            os.killpg(pgid, signal.SIGKILL)
            self.proc.wait(timeout=STOP_GRACE_SECONDS)
        except ProcessLookupError:
            pass  # already gone
        return "stopped"

    def poll_status(self) -> dict:
        if self.proc is None:
            return {"status": "idle"}
        rc = self.proc.poll()
        if rc is None:
            return {"status": "running", "pid": self.proc.pid}
        return {"status": "exited", "pid": self.proc.pid, "returncode": rc}


def main() -> None:
    p = argparse.ArgumentParser(description="Fleet mission launch bridge for argOS")
    p.add_argument("--cloud-url", default=DEFAULT_CLOUD)
    p.add_argument("--poll-hz", type=float, default=DEFAULT_POLL_HZ)
    args = p.parse_args()

    session = requests.Session()
    if BRIDGE_PASSWORD:
        session.headers["X-Bridge-Password"] = BRIDGE_PASSWORD

    mission = MissionProcess()
    interval = 1.0 / max(0.2, args.poll_hz)
    last_reported: str | None = None

    def report_if_changed(message: str | None = None) -> None:
        nonlocal last_reported
        status = mission.poll_status()
        key = status.get("status")
        if key == last_reported and not message:
            return
        body = dict(status)
        if message:
            body["message"] = message
        try:
            session.post(
                f"{args.cloud_url}/mission/launch/status", json=body, timeout=3.0
            )
        except Exception as e:
            log.warning(f"status report failed: {e}")
        last_reported = key

    def _shutdown(sig, frame):
        log.info("stopping -- stopping any running mission first")
        mission.stop()
        report_if_changed("fleet_bridge shutting down")
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info(f"cloud={args.cloud_url} poll_hz={args.poll_hz}")
    while True:
        try:
            r = session.get(f"{args.cloud_url}/mission/launch/command", timeout=3.0)
            if r.ok:
                cmd = r.json() or {}
                action = cmd.get("action")
                if action == "launch":
                    ok, msg = mission.start(
                        cmd.get("yaml", ""), cmd.get("params") or {}
                    )
                    log.info(msg)
                    report_if_changed(msg)
                elif action == "stop":
                    msg = mission.stop()
                    log.info(msg)
                    report_if_changed(msg)
        except Exception as e:
            log.warning(f"command poll failed: {e}")

        # Always re-check status, not just after a command -- catches the
        # process exiting/crashing on its own so the browser still sees it
        # even when nobody clicked "stop".
        report_if_changed()
        time.sleep(interval)


if __name__ == "__main__":
    main()
