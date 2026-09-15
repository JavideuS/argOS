"""EasyNav backend -- not implemented yet.

Placeholder so `backend: easynav` is a valid, one-line config switch once
easynav is actually in play. Fill in real topic names then; ros2_bridge.py
needs no other changes to support it since it only depends on NavBackend's
interface (see base.py). Pose doesn't need anything here at all -- it comes
from the standard `map -> base_footprint` TF lookup, same as nav2.
"""

from __future__ import annotations

from .base import NavBackend


class EasyNavBackend(NavBackend):
    name = "easynav"

    def path_topic(self) -> str:
        raise NotImplementedError("easynav backend not implemented yet")

    def path_msg_type(self):
        raise NotImplementedError("easynav backend not implemented yet")

    def map_topic(self) -> str:
        raise NotImplementedError("easynav backend not implemented yet")

    def map_msg_type(self):
        raise NotImplementedError("easynav backend not implemented yet")
