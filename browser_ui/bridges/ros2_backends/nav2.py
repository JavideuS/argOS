"""Nav2 backend

Each robot's own rclpy.Node is created with namespace=<robot's namespace>
(see ros2_bridge.py), so these are relative topic names -- ROS2 resolves
`received_global_plan` to `/ranger_1/received_global_plan` etc. on its own.
If a particular bringup remaps these differently, override per-robot via the
`topics:` block in the fleet YAML rather than editing this file.
"""

from __future__ import annotations

from nav_msgs.msg import OccupancyGrid, Path

from .base import NavBackend


class Nav2Backend(NavBackend):
    name = "nav2"

    def path_topic(self) -> str:
        # NOT planner_server's `/plan` -- fleet-coordinator's dispatch.py
        # deliberately skips bt_navigator/ComputePathToPose/planner_server
        # and drives FollowPath directly with Spooky's (smoothed) path (see
        # fleet-coordinator/README.md section 2.1), so planner_server never
        # runs and `/plan` never gets published in this architecture.
        # `received_global_plan` is nav2_controller's own echo of whatever
        # path FollowPath was actually handed -- the one that's live here.
        return "received_global_plan"

    def path_msg_type(self):
        return Path

    def map_topic(self) -> str:
        return "map"

    def map_msg_type(self):
        return OccupancyGrid
