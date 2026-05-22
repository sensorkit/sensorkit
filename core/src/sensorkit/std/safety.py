from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.auto.constraint import Constraint, ConstraintEvaluator
from sensorkit.core.client import SensorKit


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


class SafetyConstraint(Constraint):
    """Constraint that monitors a BasicSafety provider and activates when conditions are unsafe."""

    kind: Literal["safety"] = "safety"
    provider: str
    time_to_live: float = 30.0

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        logger.debug(f"monitoring safety constraint from {self.provider}")

        async with asyncio.timeout(self.time_to_live) as timeout:
            provider = kit.entity(self.provider)
            stream = await provider.monitor(BasicSafety)

            async for _, safety in stream:
                if not safety.is_safe:
                    changed = evaluator.constrain("unsafe")
                    if changed:
                        logger.info(f"Setting safety constraint from {self.provider}")
                else:
                    if evaluator.is_active:
                        logger.info(f"Clearing safety constraint from {self.provider}")
                        evaluator.clear("safe")

                evaluator.ready()
                timeout.reschedule(asyncio.get_running_loop().time() + self.time_to_live)
