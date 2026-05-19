from pydantic import BaseModel

import sensorkit.api as sk


@sk.declare_keyword
class BasicSafety(BaseModel):
    """Simple safety monitor keyword indicating whether conditions are currently safe to operate."""
    is_safe: bool


SafetyProvider = sk.declare_trait(
    "SafetyProvider",
    required_keywords=("BasicSafety",),
)

StandardSafety = sk.declare_archetype(
    "safety",
    required_traits=(SafetyProvider,),
)
"""Standard archetype for safety / go-no-go providers."""
