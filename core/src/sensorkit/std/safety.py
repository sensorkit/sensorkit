# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Literal, override

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.auto.constraint import Constraint, ConstraintEvaluator
from sensorkit.common.keyword import validate_keyword_json
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

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        provider = kit.entity(self.provider)
        consumer = await provider._stream.consume("BasicSafety")

        async for msg in consumer:
            try:
                safety = validate_keyword_json("BasicSafety", msg.data)
            except Exception:
                continue

            if not safety.is_safe:
                evaluator.constrain("unsafe")
            else:
                evaluator.clear("safe")

            evaluator.ready()
