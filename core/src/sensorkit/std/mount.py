"""Standard mount-related traits and archetype."""

import sensorkit.api as sk
from sensorkit.models.devices import (
    Deinit,
    DisableAxis,
    EnableAxis,
    FollowTarget,
    Home,
    Init,
    MoveToPark,
    SetParkPosition,
    Stop,
)

StandardMount = sk.declare_archetype(
    "mount",
    required_commands=(
        Init,
        Deinit,
        MoveToPark,
        SetParkPosition,
        EnableAxis,
        DisableAxis,
        Stop,
        Home,
        FollowTarget,
    ),
)
