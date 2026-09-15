"""Uniform interface a navigation-stack backend must implement.

ros2_bridge.py looks up `map -> base_footprint`
via TF directly, the same way for every backend, rather than going through a
backend-specific pose topic. `base_footprint` is specifically the robot's
ground projection with roll/pitch zeroed (REP-120).

What genuinely differs per nav stack is topic *naming* for path and map, so
that's what's left here. Adding a new nav stack means adding a new backend
module and registering it in `ros2_backends/__init__.py`'s `BACKENDS` --
ros2_bridge.py's core loop never needs to change.

Every robot gets its own namespaced rclpy.Node (see ros2_bridge.py), so
these return *relative* topic names -- ROS2 resolves them against that
node's namespace automatically. Override per-robot via the fleet YAML's
`topics:` block if a bringup needs something else entirely (relative or
absolute).
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class NavBackend(ABC):
    name: str

    @abstractmethod
    def path_topic(self) -> str:
        """Relative topic carrying this robot's currently tracked global plan."""

    @abstractmethod
    def path_msg_type(self):
        """ROS2 message class for `path_topic`."""

    @abstractmethod
    def map_topic(self) -> str:
        """Relative topic carrying the static occupancy map."""

    @abstractmethod
    def map_msg_type(self):
        """ROS2 message class for `map_topic`."""
