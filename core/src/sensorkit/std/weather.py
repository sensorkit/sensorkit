# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from typing import Literal, override

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.auto.constraint import Constraint, ConstraintEvaluator
from sensorkit.common.keyword import validate_keyword_json
from sensorkit.core.client import SensorKit


@sk.declare_keyword
class BasicWeather(BaseModel):
    """Ambient weather conditions keyword, with all fields optional."""
    temperature: float | None = None
    humidity: float | None = None
    pressure: float | None = None
    cloud_cover: float | None = None
    dew_point: float | None = None
    rain_rate: float | None = None
    wind_direction: float | None = None
    wind_speed: float | None = None


WeatherProvider = sk.declare_trait(
    "WeatherProvider",
    required_keywords=("BasicWeather",),
)

StandardWeather = sk.declare_archetype(
    "weather",
    required_traits=(WeatherProvider,),
)
"""Standard archetype for ambient weather telemetry providers."""


class WeatherFieldEvaluator:

    def __init__(self, name: str, threshold: float, deadband: float):
        self.name = name
        self.threshold = threshold
        self.deadband = deadband
        self._exceeded = False

    def eval_threshold(self, weather: BasicWeather) -> float:
        value: float | None = getattr(weather, self.name, None)

        if value is None:
            return float("inf")

        over = value - self.threshold
        over_deadband = over + self.deadband
        self._exceeded = (over_deadband if self._exceeded else over) > 0

        return over_deadband if self._exceeded else over


class WeatherConstraint(Constraint):
    """Constraint that monitors a weather provider and activates when conditions exceed thresholds."""

    kind: Literal["weather"] = "weather"
    provider: str
    humidity_max: float | None = None
    humidity_deadband: float = 0.0
    wind_max: float | None = None
    wind_deadband: float = 0.0
    rain_max: float | None = None
    rain_deadband: float = 0.0

    def _get_field_evaluators(self):
        if self.humidity_max is not None:
            yield WeatherFieldEvaluator("humidity", self.humidity_max, self.humidity_deadband)

        if self.wind_max is not None:
            yield WeatherFieldEvaluator("wind_speed", self.wind_max, self.wind_deadband)

        if self.rain_max is not None:
            yield WeatherFieldEvaluator("rain_rate", self.rain_max, self.rain_deadband)

    @functools.cached_property
    def _field_evaluators(self) -> tuple[WeatherFieldEvaluator, ...]:
        return tuple(self._get_field_evaluators())

    def check_weather(self, weather: BasicWeather) -> list[str]:
        errors = []

        for field in self._field_evaluators:
            delta = field.eval_threshold(weather)

            if delta == float("inf"):
                errors.append(f"{field.name} data missing")
            elif delta >= 0:
                errors.append(f"{field.name} is {delta:.1f} too high")

        return errors

    @override
    async def check_task(self, evaluator: ConstraintEvaluator, kit: SensorKit):
        provider = kit.entity(self.provider)
        consumer = await provider._stream.consume("BasicWeather")

        async for msg in consumer:
            try:
                weather = validate_keyword_json("BasicWeather", msg.data)
            except Exception:
                continue

            errors = self.check_weather(weather)

            if errors:
                evaluator.constrain(", ".join(errors))
            else:
                evaluator.clear()

            evaluator.ready()
