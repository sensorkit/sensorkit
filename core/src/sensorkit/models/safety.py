"""Standard safety keyword and archetype."""

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.common.keyword import declare_keyword


@declare_keyword
class BasicSafety(BaseModel):
    """Simple safety monitor keyword indicating whether conditions are currently safe to operate."""
    is_safe: bool


StandardSafety = sk.declare_archetype(
    "safety",
    required_keywords=(BasicSafety,),
)
"""Standard archetype for safety / go-no-go providers.

A StandardSafety entity publishes ``BasicSafety`` (a single boolean is-safe signal indicating
whether external conditions allow operation, e.g. AAG CloudWatcher's roof-safe flag). It's a
capability tag for discovery — safety providers are telemetry sources, not actuators, so the
archetype asserts no required commands.
"""
