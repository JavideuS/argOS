"""Navigation-stack backends for ros2_bridge.py -- see base.py for the interface."""

from .base import NavBackend
from .easynav import EasyNavBackend
from .nav2 import Nav2Backend

BACKENDS: dict[str, type[NavBackend]] = {
    "nav2": Nav2Backend,
    "easynav": EasyNavBackend,
}

__all__ = ["NavBackend", "Nav2Backend", "EasyNavBackend", "BACKENDS"]
