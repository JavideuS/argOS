"""
Costmap / path bridge — connects to DimOS Socket.IO visualization module
(port 7779) and relays costmap and path data to the ArgOS cloud server.

Handles events: costmap, path, goal_reached, navigation_state, full_state.
Pose and camera are handled by nav_bridge and camera_bridge respectively.

Must run alongside DimOS's WebsocketVisModule (default port 7779).

    python costmap_bridge.py --cloud-url http://localhost:8080
    python costmap_bridge.py --ws-url http://localhost:7779 --cloud-url http://<ec2-ip>:8080

Display modes (--display-mode):
    both      — push costmap heatmap + path (default)
    heatmap   — push costmap heatmap + path  (alias for both)
    pcl       — skip costmap; UI shows pointcloud from pc_bridge instead
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time

import httpx

try:
    import socketio
    HAS_SOCKETIO = True
except ImportError:
    HAS_SOCKETIO = False

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_WS_URL  = os.environ.get("DIMOS_WS_URL",  "http://localhost:7779")
DEFAULT_CLOUD   = os.environ.get("CLOUD_URL",      "http://localhost:8080")
DEFAULT_ROBOT   = os.environ.get("ROBOT_ID",       "go2_a")
_BRIDGE_PW      = os.environ.get("BRIDGE_PASSWORD", "")

# Costmap frontend polls at 1 Hz — no point pushing faster.
_COSTMAP_MIN_INTERVAL = 0.9
# Path/goal: push up to 20 Hz so goal markers appear instantly after a click.
_PATH_MIN_INTERVAL = 0.05


def _headers(robot_id: str) -> dict:
    h = {"X-Robot-Id": robot_id}
    if _BRIDGE_PW:
        h["X-Bridge-Password"] = _BRIDGE_PW
    return h


async def _post(client: httpx.AsyncClient, url: str, payload: dict, robot_id: str) -> None:
    try:
        await client.post(url, json=payload, headers=_headers(robot_id), timeout=2.0)
    except Exception as e:
        logger.debug(f"push failed: {e}")


async def listen(ws_url: str, cloud_url: str, robot_id: str, display_mode: str) -> None:
    if not HAS_SOCKETIO:
        logger.error("socketio not installed — run: pip install 'python-socketio[asyncio_client]'")
        return

    send_heatmap = display_mode != "pcl"

    http_url = ws_url.replace("ws://", "http://").replace("wss://", "https://")

    while True:
        sio = socketio.AsyncClient(reconnection=False, logger=False, engineio_logger=False)

        # Per-connection rate-limit timestamps.
        _last_costmap_push: list[float] = [0.0]
        _last_path_push:    list[float] = [0.0]

        @sio.event
        async def connect():
            logger.info(f"Connected to DimOS at {http_url}")

        @sio.event
        async def disconnect():
            logger.info("Disconnected from DimOS")

        @sio.event
        async def costmap(data):
            if not send_heatmap:
                return
            now = time.time()
            if now - _last_costmap_push[0] < _COSTMAP_MIN_INTERVAL:
                return
            _last_costmap_push[0] = now
            async with httpx.AsyncClient() as c:
                await _post(c, f"{cloud_url}/ingest/costmap",
                            {"costmap": data}, robot_id)

        @sio.event
        async def path(data):
            now = time.time()
            if now - _last_path_push[0] < _PATH_MIN_INTERVAL:
                return
            _last_path_push[0] = now
            async with httpx.AsyncClient() as c:
                await _post(c, f"{cloud_url}/ingest/path",
                            {"path": data}, robot_id)

        @sio.event
        async def goal_reached(data):
            async with httpx.AsyncClient() as c:
                await _post(c, f"{cloud_url}/ingest/path",
                            {"path": {"points": []}}, robot_id)

        @sio.event
        async def navigation_state(data):
            state_str = data if isinstance(data, str) else str(data.get("data", "idle"))
            logger.info(f"Navigation state: {state_str}")

        @sio.event
        async def full_state(data):
            if not isinstance(data, dict):
                return
            async with httpx.AsyncClient() as c:
                now = time.time()
                if send_heatmap and "costmap" in data:
                    if now - _last_costmap_push[0] >= _COSTMAP_MIN_INTERVAL:
                        _last_costmap_push[0] = now
                        await _post(c, f"{cloud_url}/ingest/costmap",
                                    {"costmap": data["costmap"]}, robot_id)
                if "path" in data:
                    if now - _last_path_push[0] >= _PATH_MIN_INTERVAL:
                        _last_path_push[0] = now
                        await _post(c, f"{cloud_url}/ingest/path",
                                    {"path": data["path"]}, robot_id)

        try:
            logger.info(f"Connecting to DimOS at {http_url}…")
            await sio.connect(http_url)
            while sio.connected:
                await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Connection error: {e} — retrying in 5s")
        finally:
            try:
                await sio.disconnect()
            except Exception:
                pass

        await asyncio.sleep(5)


def main() -> None:
    p = argparse.ArgumentParser(description="DimOS costmap/path → ArgOS cloud")
    p.add_argument("--ws-url",    default=DEFAULT_WS_URL,
                   help="DimOS Socket.IO URL (default: http://localhost:7779)")
    p.add_argument("--cloud-url", default=DEFAULT_CLOUD,
                   help="ArgOS cloud server URL (default: http://localhost:8080)")
    p.add_argument("--robot-id",  default=DEFAULT_ROBOT,
                   help="Robot identifier (default: go2_a)")
    p.add_argument("--display-mode", default="both",
                   choices=["both", "heatmap", "pcl"],
                   help="'both'/'heatmap' push costmap heatmap; 'pcl' skips costmap (saves bandwidth when UI shows pointcloud)")
    args = p.parse_args()
    logger.info(f"ws={args.ws_url}  cloud={args.cloud_url}  robot={args.robot_id}  display={args.display_mode}")
    try:
        asyncio.run(listen(args.ws_url, args.cloud_url, args.robot_id, args.display_mode))
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
