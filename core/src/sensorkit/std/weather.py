# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Literal, TypedDict, override

from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.auto.constraint import Constraint, ConstraintEvaluator
from sensorkit.common.keyword import validate_keyword_json
from sensorkit.core.client import SensorKit

if TYPE_CHECKING:
    import astropy.units as u


class RefractionArgs(TypedDict):
    temperature: u.Quantity | None
    relative_humidity: u.Quantity | None
    pressure: u.Quantity | None


@sk.declare_keyword
class BasicWeather(BaseModel):
    """Ambient weather conditions keyword, with all fields optional.

    Units follow the ASCOM IObservingConditions convention.

    Attributes:
        temperature: Ambient air temperature, °C.
        humidity: Relative humidity, percent (0-100).
        pressure: Barometric pressure, hPa, absolute at the observatory altitude rather
            than corrected to sea level.
        cloud_cover: Sky covered by cloud, percent (0-100).
        dew_point: Dew point temperature, °C.
        rain_rate: Rainfall intensity, mm/h.
        wind_direction: Direction the wind blows from, degrees clockwise from true north
            (0-360).
        wind_speed: Wind speed, m/s.
    """
    temperature: float | None = None
    humidity: float | None = None
    pressure: float | None = None
    cloud_cover: float | None = None
    dew_point: float | None = None
    rain_rate: float | None = None
    wind_direction: float | None = None
    wind_speed: float | None = None

    def to_astropy(self) -> RefractionArgs:
        """Return the refraction arguments accepted by astropy's AltAz frame.

        Keys match the AltAz frame attributes, so the result can be splatted straight
        into AltAz or into SkyCoord with an AltAz frame. Astropy normalizes the percent
        humidity to the 0-1 fraction it works in.
        """
        import astropy.units as u

        return RefractionArgs(
            temperature=self.temperature * u.deg_C if self.temperature is not None else None,
            relative_humidity=self.humidity * u.percent if self.humidity is not None else None,
            pressure=self.pressure * u.hPa if self.pressure is not None else None,
        )


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
    """Constraint that monitors a weather provider and activates when conditions exceed thresholds.

    Thresholds and deadbands are compared directly against the matching BasicWeather field
    and so carry its units: humidity in percent, wind speed in m/s, rain rate in mm/h.

    Attributes:
        provider: Entity name of the weather provider to consume BasicWeather from.
        humidity_max: Relative humidity ceiling, percent.
        humidity_deadband: Percent below humidity_max the reading must fall to clear.
        wind_max: Wind speed ceiling, m/s.
        wind_deadband: m/s below wind_max the reading must fall to clear.
        rain_max: Rainfall intensity ceiling, mm/h.
        rain_deadband: mm/h below rain_max the reading must fall to clear.
    """

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
