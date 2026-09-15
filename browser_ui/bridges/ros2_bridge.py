"""
Multi-robot ROS2 fleet bridge -- same cloud push contract as nav_bridge.py
(`X-Robot-Id` header, POST /ingest/pose + /ingest/path) but sourced from
ROS2 topics/TF via rclpy instead of DimOS/LCM.

Each robot gets its own namespaced rclpy.Node (see RobotBridge) -- this
bringup publishes fully separate per-robot TF topics (`/ranger_1/tf`, not a
shared `/tf`), so a single shared node/buffer couldn't see either robot's
frames. All nodes share one MultiThreadedExecutor.

Pose comes from TF (`map -> base_footprint`, REP-105/120's standard ground
frame), not a topic.
Path and the static map are still backend-specific topics.

v1 is read-only telemetry (pose + path + one shared static map upstream) --
no goal-forwarding. Goal-setting stays fleet-coordinator's job (mission YAML
-> Spooky -> dispatch.py's FollowPath) so there's no second path that could
race with its release-gating.

Costmap ingestion is deliberately not included yet: /ingest/costmap's
full/delta voxel-update protocol needs its own careful adapter (the static
map below is push-once, genuinely simpler, and doesn't need it).
Still thinking on shared map + multi-robot sensor fusion, and independent
per-robot view.

Standalone:
    python ros2_bridge.py --config ../config/ros2_fleet.example.yaml
"""

from __future__ import annotations

import argparse
import base64
import logging
import math
import os
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml
import rclpy
import tf2_ros
from rclpy.duration import Duration
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy

from ros2_backends import BACKENDS

logging.basicConfig(level=logging.INFO, format="[ros2_bridge] %(message)s")
log = logging.getLogger(__name__)

DEFAULT_CLOUD = os.environ.get("CLOUD_URL", "http://localhost:8080")
DEFAULT_POSE_HZ = float(os.environ.get("ROS2_POSE_HZ", "15"))
# Matches the dashboard's own path poll interval (250ms) -- pushing faster
# than the UI polls is wasted work, and nav2_controller republishes
# received_global_plan at control-rate (~20Hz) while a goal is active, so
# this also caps how much of that we forward.
DEFAULT_PATH_HZ = float(os.environ.get("ROS2_PATH_HZ", "4"))
BRIDGE_PASSWORD = os.environ.get("BRIDGE_PASSWORD", "")


# ── Per-robot config ─────────────────────────────────────────


@dataclass
class RobotConfig:
    """One robot's bridge-side parameters.

    fleet_coordinator.coordinator_node publishes each robot's
    seeded initial pose to `/{mission Robot.id}/initialpose` and looks up
    its TF the same way -- so a mission robot's `id` *must* equal its real
    ROS2 namespace or coordinator_node ends up talking to a namespace
    nothing publishes on.

    `robot_radius`/`inflation` default to fleet_coordinator.robot.Robot's
    own defaults (0.35 / 0.0) so a homogeneous fleet only needs namespace;
    override per robot for a heterogeneous fleet, or when this bridge needs
    to hand these straight to Spooky later (see fleet-coordinator/robot.py).
    `urdf_path` is carried through for the browser's future 3D render --
    unused by this bridge beyond a sanity check that the file exists.
    `base_frame` is the TF frame looked up as `map -> base_frame` for this
    robot's ground pose -- REP-120's `base_footprint` by default, but not
    every URDF publishes one (simple wheeled platforms often skip it and
    keep `base_link` already ground-referenced); override if yours does.
    `map_source` marks this robot's own `/map` as the one the fleet's
    shared static-map layer is pulled from (see MapBridge) -- at most one
    robot should set it; if none do, the first robot in the fleet is used.
    """

    namespace: str
    backend: str = "nav2"
    robot_radius: float = 0.35
    inflation: float = 0.0
    urdf_path: str | None = None  # local xacro/urdf source, sanity-checked below only
    model: str | None = None  # browser-facing slug served at argOS's /models/<model>/
    base_frame: str = "base_footprint"
    map_source: bool = False
    topics: dict = field(default_factory=dict)  # per-robot topic-name overrides

    def __post_init__(self) -> None:
        if self.backend not in BACKENDS:
            raise ValueError(
                f"robot {self.namespace!r}: unknown backend {self.backend!r}, "
                f"expected one of {sorted(BACKENDS)}"
            )
        if self.urdf_path and not Path(self.urdf_path).exists():
            log.warning(
                f"[{self.namespace}] urdf_path does not exist: {self.urdf_path}"
            )


def load_fleet_yaml(path: str | Path) -> tuple[dict, list[RobotConfig]]:
    """Load a fleet bridge config: {cloud_url, pose_hz, robots: [...]}.
    See config/ros2_fleet.example.yaml for a worked example -- `robots`
    entries mirror fleet-coordinator's mission YAML shape (id/robot_radius/
    inflation) plus this bridge's own fields (namespace/backend/urdf_path).
    """
    with open(path) as f:
        doc = yaml.safe_load(f) or {}
    specs = doc.get("robots", [])
    if not isinstance(specs, list):
        raise ValueError(
            f"ros2_bridge: {path!s} 'robots' must be a list, got {type(specs).__name__}"
        )
    robots = [RobotConfig(**spec) for spec in specs]
    return doc, robots


def _yaw_from_quaternion(q) -> float:
    """Standard yaw-from-quaternion (Z-axis rotation); pitch/roll are left
    at 0.0 -- ground robots don't need them and the full orientation is
    still carried through via qx/qy/qz/qw regardless."""
    siny_cosp = 2.0 * (q.w * q.z + q.x * q.y)
    cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z)
    return math.atan2(siny_cosp, cosy_cosp)


def _bucket_occupancy(v: int) -> int:
    """nav_msgs/OccupancyGrid cell -> one byte: 0=free, 1=occupied, 2=unknown.
    -1 is ROS's "unknown"; 0-100 is occupancy probability, >=50 treated as
    occupied (nav2's own costmap default lethal-ish threshold territory)."""
    if v < 0:
        return 2
    return 1 if v >= 50 else 0


# ── Per-robot bridge ─────────────────────────────────────────


class RobotBridge:
    """One robot's own namespaced Node, TF-based pose, and path subscription.

    Each robot owns a full rclpy.Node(namespace=config.namespace) rather
    than sharing one -- this bringup's per-robot TF topics
    (`/ranger_1/tf`, not a shared `/tf`) mean a shared node's TF buffer
    would never see a given robot's frames; giving each robot its own node
    makes relative topic/frame names ("tf", "map", "base_footprint")
    resolve correctly on their own. It also means MultiThreadedExecutor can
    run different robots' callbacks fully concurrently with no manual
    callback-group bookkeeping -- separate nodes are already independent.
    """

    def __init__(
        self,
        config: RobotConfig,
        cloud_url: str,
        pose_hz: float,
        path_hz: float,
    ):
        self.config = config
        self.cloud_url = cloud_url
        self.backend = BACKENDS[config.backend]()
        self._pose_interval = 1.0 / max(1.0, pose_hz)
        self._path_interval = 1.0 / max(1.0, path_hz)
        self._last_pose_push = 0.0
        self._last_path_push = 0.0
        self._got_pose = False
        self._got_path = False
        self._session = requests.Session()
        if BRIDGE_PASSWORD:
            self._session.headers["X-Bridge-Password"] = BRIDGE_PASSWORD
        self._session.headers["X-Robot-Id"] = config.namespace

        # TransformListener always subscribes to the ABSOLUTE topics '/tf'
        # and '/tf_static' (hardcoded in tf2_ros -- this rclpy/tf2_ros
        # release doesn't expose a tf_topic=/tf_static_topic= override the
        # way a newer one does), and an absolute name is never affected by
        # this node's namespace= -- so without the explicit remap below,
        # this buffer would silently listen on the *global* /tf forever and
        # never see anything published under /<namespace>/tf, exactly like
        # `ros2 topic echo /tf` sees nothing here without the equivalent
        # `-r /tf:=/<namespace>/tf` on the CLI. Passing that same remap as
        # cli_args reproduces it for this node specifically
        self.node = Node(
            f"argos_bridge_{config.namespace}",
            namespace=config.namespace,
            cli_args=[
                "--ros-args",
                "-r",
                f"/tf:=/{config.namespace}/tf",
                "-r",
                f"/tf_static:=/{config.namespace}/tf_static",
            ],
        )

        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self.node)
        self.node.create_timer(self._pose_interval, self._poll_pose)

        path_topic = config.topics.get("path") or self.backend.path_topic()
        self.node.create_subscription(
            self.backend.path_msg_type(), path_topic, self._on_path, 10
        )
        log.info(
            f"[{config.namespace}] backend={self.backend.name} ns={config.namespace} "
            f"pose<-TF(map->{config.base_frame}) path<-{path_topic}"
        )

    def _push(self, endpoint: str, payload: dict) -> None:
        # A bridge that silently drops every push is worse than
        # one that's noisy about it.
        try:
            r = self._session.post(
                f"{self.cloud_url}{endpoint}", json=payload, timeout=1.5
            )
            if not r.ok:
                log.warning(
                    f"[{self.config.namespace}] {endpoint} -> {r.status_code}: {r.text[:200]}"
                )
        except Exception as e:
            log.warning(f"[{self.config.namespace}] push {endpoint} failed: {e}")

    def _poll_pose(self) -> None:
        try:
            tf = self.tf_buffer.lookup_transform(
                "map",
                self.config.base_frame,
                rclpy.time.Time(),
                timeout=Duration(seconds=0.05),
            )
        except tf2_ros.TransformException:
            # Not localized yet / base_frame not in this URDF's TF tree --
            # the startup watchdog below calls out persistent absence.
            return
        if not self._got_pose:
            self._got_pose = True
            log.info(f"[{self.config.namespace}] first pose received")
        now = time.time()
        if now - self._last_pose_push < self._pose_interval:
            return
        self._last_pose_push = now
        t = tf.transform.translation
        q = tf.transform.rotation
        self._push(
            "/ingest/pose",
            {
                "x": t.x,
                "y": t.y,
                "z": t.z,
                "yaw": _yaw_from_quaternion(q),
                "pitch": 0.0,
                "roll": 0.0,
                "qx": q.x,
                "qy": q.y,
                "qz": q.z,
                "qw": q.w,
                "ts": now,
                "robot_radius": self.config.robot_radius,
                "inflation": self.config.inflation,
                "model": self.config.model,
            },
        )

    def _on_path(self, msg) -> None:
        if not self._got_path:
            self._got_path = True
            log.info(
                f"[{self.config.namespace}] first path received ({len(msg.poses)} poses)"
            )
        # received_global_plan republishes at nav2_controller's control rate
        # (~20Hz) while a goal is active -- without this, every one of those
        # messages fired a blocking HTTP POST from this callback, which
        # (before each robot got its own node/executor slice) starved
        # other robots' callbacks (looked like "only one robot's path ever
        # shows up").
        now = time.time()
        if now - self._last_path_push < self._path_interval:
            return
        # received_global_plan's own poses come stamped in msg.header.frame_id
        # (this backend: base_link, the robot's own body frame -- NOT map),
        # so pose.pose.position is relative to wherever the robot currently
        # is, not world-fixed. Pushed raw, every point renders as if the
        # robot's own frame origin *is* the map origin -- the plan's shape
        # still looks right (frame-relative geometry is unaffected) but it
        # always visually starts at (0, 0) instead of at the robot. Rotate +
        # translate through map -> frame_id (same "latest available" TF
        # lookup _poll_pose already uses, for the same extrapolation-safety
        # reason) to place the plan in world coordinates before pushing.
        try:
            tf = self.tf_buffer.lookup_transform(
                "map",
                msg.header.frame_id,
                rclpy.time.Time(),
                timeout=Duration(seconds=0.05),
            )
        except tf2_ros.TransformException:
            return
        self._last_path_push = now
        tx, ty = tf.transform.translation.x, tf.transform.translation.y
        yaw = _yaw_from_quaternion(tf.transform.rotation)
        cos_yaw, sin_yaw = math.cos(yaw), math.sin(yaw)
        points = []
        for pose in msg.poses:
            lx, ly = pose.pose.position.x, pose.pose.position.y
            points.append(
                [
                    tx + lx * cos_yaw - ly * sin_yaw,
                    ty + lx * sin_yaw + ly * cos_yaw,
                ]
            )
        # Final waypoint's own heading, world-frame -- same rotate-by-yaw as
        # the positions above, just applied to its orientation instead of
        # its translation (2D: world_yaw = transform_yaw + local_yaw). Only
        # the last pose is needed
        # This drives the goal marker's arrow (theta, not just position),
        # not the path line itself.
        goal_yaw = None
        if msg.poses:
            goal_yaw = yaw + _yaw_from_quaternion(msg.poses[-1].pose.orientation)
        self._push("/ingest/path", {"path": {"points": points, "goal_yaw": goal_yaw}})


# ── Shared static map (one source robot) ───────────────────────


class MapBridge:
    """Pushes ONE robot's static /map once (and again if it ever changes) as
    the fleet's shared map layer.

    Reuses the source robot's own node (already namespaced correctly) rather
    than creating another one.
    """

    def __init__(self, node: Node, backend, cloud_url: str, robot_id: str):
        self.node = node
        self.cloud_url = cloud_url
        self.robot_id = robot_id
        self._session = requests.Session()
        if BRIDGE_PASSWORD:
            self._session.headers["X-Bridge-Password"] = BRIDGE_PASSWORD

        # map_server publishes /map latched (TRANSIENT_LOCAL) so a
        # late-joining subscriber still gets it
        qos = QoSProfile(
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            depth=1,
        )
        node.create_subscription(
            backend.map_msg_type(), backend.map_topic(), self._on_map, qos
        )
        log.info(f"[map] source={robot_id} topic={backend.map_topic()}")

    def _on_map(self, msg) -> None:
        w, h = msg.info.width, msg.info.height
        data = bytes(_bucket_occupancy(v) for v in msg.data)
        payload = {
            "width": w,
            "height": h,
            "resolution": msg.info.resolution,
            "origin": {
                "x": msg.info.origin.position.x,
                "y": msg.info.origin.position.y,
            },
            "data": base64.b64encode(zlib.compress(data)).decode(),
        }
        try:
            r = self._session.post(
                f"{self.cloud_url}/ingest/map", json=payload, timeout=5.0
            )
            if r.ok:
                log.info(
                    f"[map] pushed {w}x{h} @ {msg.info.resolution}m/cell from {self.robot_id}"
                )
            else:
                log.warning(f"[map] push -> {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log.warning(f"[map] push failed: {e}")


# ── Fleet supervisor + entrypoint ───────────────────────────────


class FleetSupervisor(Node):
    """Not namespaced -- just hosts the startup-check timer. Each robot's
    own telemetry lives on its own RobotBridge/Node now."""

    def __init__(self, robot_bridges: list[RobotBridge]):
        super().__init__("argos_fleet_supervisor")
        self.robot_bridges = robot_bridges
        self._startup_check_timer = self.create_timer(5.0, self._check_startup)

    def _check_startup(self) -> None:
        """One-shot: 5s after launch, call out any robot that never fired its
        pose callback -- e.g. AMCL not localized yet / base_frame not in
        this URDF's TF tree / no initial pose set. Silence here otherwise
        looks identical to "everything's fine, just no data yet"."""
        self._startup_check_timer.cancel()
        silent = [rb.config.namespace for rb in self.robot_bridges if not rb._got_pose]
        if silent:
            log.warning(
                f"no pose received yet for: {', '.join(silent)} -- check that "
                f"`map -> <base_frame>` actually exists in TF "
                f"(`ros2 topic echo /<namespace>/tf_static --once`), and that "
                f"AMCL has a valid initial pose for that robot"
            )


def main():
    p = argparse.ArgumentParser(description="Multi-robot ROS2 fleet bridge for argOS")
    p.add_argument(
        "--config",
        required=True,
        help="fleet YAML (see config/ros2_fleet.example.yaml)",
    )
    p.add_argument("--cloud-url", default=None, help="overrides cloud_url from config")
    p.add_argument(
        "--pose-hz", type=float, default=None, help="overrides pose_hz from config"
    )
    p.add_argument(
        "--path-hz", type=float, default=None, help="overrides path_hz from config"
    )
    args = p.parse_args()

    fleet_cfg, robots = load_fleet_yaml(args.config)
    if not robots:
        raise SystemExit(f"ros2_bridge: no robots defined in {args.config}")
    cloud_url = args.cloud_url or fleet_cfg.get("cloud_url", DEFAULT_CLOUD)
    pose_hz = args.pose_hz or float(fleet_cfg.get("pose_hz", DEFAULT_POSE_HZ))
    path_hz = args.path_hz or float(fleet_cfg.get("path_hz", DEFAULT_PATH_HZ))

    rclpy.init()

    robot_bridges = [RobotBridge(cfg, cloud_url, pose_hz, path_hz) for cfg in robots]

    map_source = next(
        (rb for rb in robot_bridges if rb.config.map_source), robot_bridges[0]
    )
    map_bridge = MapBridge(
        map_source.node, map_source.backend, cloud_url, map_source.config.namespace
    )

    supervisor = FleetSupervisor(robot_bridges)
    log.info(
        f"cloud={cloud_url} pose_hz={pose_hz} path_hz={path_hz} "
        f"robots={[r.namespace for r in robots]} map_source={map_source.config.namespace}"
    )

    # MultiThreadedExecutor hosting every robot's own node plus the
    # supervisor -- each robot's blocking HTTP _push() only ever occupies
    # its own node's slice of the thread pool, never stalls another robot.
    executor = MultiThreadedExecutor()
    executor.add_node(supervisor)
    for rb in robot_bridges:
        executor.add_node(rb.node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        executor.shutdown()
        supervisor.destroy_node()
        for rb in robot_bridges:
            rb.node.destroy_node()
        # rclpy's own SIGINT handler can already have shut the context down
        # by the time we get here (that's what raised the KeyboardInterrupt
        # above) -- calling shutdown() again unconditionally throws.
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
