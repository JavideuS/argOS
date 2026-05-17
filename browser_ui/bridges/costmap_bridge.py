"""
DimOS costmap bridge — subscribes to DimOS Socket.IO `costmap` event
and forwards it to the cloud server's /ingest/costmap endpoint.

Extracted from ws_bridge.py. All other functionality from that file
(pose, path, camera, goal forwarding) is handled by nav_bridge.py and
camera_bridge.py.

Must run alongside DimOS's WebsocketVisModule (default port 7779).

    python costmap_bridge.py --cloud-url http://localhost:8080
    python costmap_bridge.py --ws-url http://localhost:7779 --cloud-url http://<ec2-ip>:8080
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

import httpx

try:
    import socketio
    HAS_SOCKETIO = True
except ImportError:
    HAS_SOCKETIO = False

logging.basicConfig(level=logging.INFO, format="[costmap_bridge] %(message)s")
log = logging.getLogger(__name__)

DEFAULT_WS_URL = os.environ.get("DIMOS_WS_URL", "http://localhost:7779")
DEFAULT_CLOUD = os.environ.get("CLOUD_URL", "http://localhost:8080")
DEFAULT_ROBOT_ID = os.environ.get("ROBOT_ID", "go2_a")
_BRIDGE_PW = os.environ.get("BRIDGE_PASSWORD", "")


def _headers() -> dict:
    return {"X-Bridge-Password": _BRIDGE_PW} if _BRIDGE_PW else {}


async def run(ws_url: str, cloud_url: str, robot_id: str) -> None:
    if not HAS_SOCKETIO:
        log.error("socketio not installed — run: pip install 'python-socketio[asyncio_client]'")
        return

    while True:
        sio = socketio.AsyncClient(reconnection=False, logger=False, engineio_logger=False)

        @sio.event
        async def connect():
            log.info(f"connected to DimOS at {ws_url}")

        @sio.event
        async def disconnect():
            log.info("disconnected from DimOS")

        @sio.event
        async def costmap(data):
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    await client.post(
                        f"{cloud_url}/ingest/costmap",
                        json={"robot_id": robot_id, "costmap": data},
                        headers=_headers(),
                    )
            except Exception as e:
                log.debug(f"costmap push failed: {e}")

        @sio.event
        async def path(data):
            # data: {"type": "path", "points": [[x, y], ...]}
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    await client.post(
                        f"{cloud_url}/ingest/path",
                        json={"robot_id": robot_id, "path": data},
                        headers=_headers(),
                    )
            except Exception as e:
                log.debug(f"path push failed: {e}")

        @sio.event
        async def goal_reached(data):
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    await client.post(
                        f"{cloud_url}/ingest/path",
                        json={"robot_id": robot_id, "path": {"points": []}},
                        headers=_headers(),
                    )
            except Exception as e:
                log.debug(f"path clear failed: {e}")

        try:
            await sio.connect(ws_url)
            while sio.connected:
                await asyncio.sleep(1)
        except Exception as e:
            log.warning(f"connection error: {e} — retrying in 5s")
        finally:
            try:
                await sio.disconnect()
            except Exception:
                pass

        await asyncio.sleep(5)


def main() -> None:
    p = argparse.ArgumentParser(description="DimOS costmap → /ingest/costmap")
    p.add_argument("--ws-url", default=DEFAULT_WS_URL,
                   help="DimOS Socket.IO URL (default: http://localhost:7779)")
    p.add_argument("--cloud-url", default=DEFAULT_CLOUD,
                   help="Cloud server URL (default: http://localhost:8080)")
    p.add_argument("--robot-id", default=DEFAULT_ROBOT_ID,
                   help="Robot identifier (default: go2_a)")
    args = p.parse_args()
    log.info(f"ws={args.ws_url} cloud={args.cloud_url} robot={args.robot_id}")
    try:
        asyncio.run(run(args.ws_url, args.cloud_url, args.robot_id))
    except KeyboardInterrupt:
        log.info("stopped")


if __name__ == "__main__":
    main()
