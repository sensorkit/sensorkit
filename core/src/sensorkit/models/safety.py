from pydantic import BaseModel

from sensorkit.common.keyword import declare_keyword


@declare_keyword
class BasicSafety(BaseModel):
    """Simple safety monitor keyword indicating whether conditions are currently safe to operate."""
    is_safe: bool
