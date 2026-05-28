from __future__ import annotations

import asyncio
from typing import Literal, override

from loguru import logger
from pydantic import BaseModel

import sensorkit.api as sk
from sensorkit.indigo.device import (
    IndigoDevice,
    IndigoDeviceConfig,
    IndigoDeviceState,
    IndigoProperty,
)
from sensorkit.models.devices import Deinit, Init
from sensorkit.std import Connected
from sensorkit.std.weather import BasicWeather, StandardWeather


@sk.declare_keyword
class IndigoWeatherStatus(BaseModel):
    """Extended weather status from an INDIGO AUX weather device.

    Publishes all items found on the AUX_WEATHER property, beyond the
    standard temperature/humidity/dewpoint. INDIGO drivers may expose
    additional items like wind, pressure, rain, sky temperature, etc.
    """

    property_state: str = "Idle"
    items: dict[str, float | str | None] = {}


@sk.declare_keyword
class WeatherConditions(BaseModel):
    """Categorical weather conditions from devices like the AAG CloudWatcher.

    Some weather stations report conditions as categorical states rather than
    raw numeric values. This keyword publishes those conditions, which can be
    used with GenericConstraint (kind: conditional, condition.kind: equals).

    All fields are optional — only populated conditions are published.
    """

    cloud: str | None = None  # Clear, Cloudy, Overcast
    humidity: str | None = None  # Humid, Normal, Dry
    rain: str | None = None  # Raining, Wet, Dry
    sky: str | None = None  # Dark, Light, Very Light
    wind: str | None = None  # Strong, Moderate, Calm


# Map from INDIGO AUX_WEATHER item names to BasicWeather fields.
_WEATHER_FIELD_MAP: dict[str, str] = {
    "TEMPERATURE": "temperature",
    "HUMIDITY": "humidity",
    "DEWPOINT": "dew_point",
    "WIND_SPEED": "wind_speed",
    "WIND_DIRECTION": "wind_direction",
    "PRESSURE": "pressure",
    "ATMOSPHERIC_PRESSURE": "pressure",
    "RAIN_RATE": "rain_rate",
    "CLOUD_COVER": "cloud_cover",
}

# Map from INDIGO property names to WeatherConditions fields.
# These are separate SwitchVector properties where the active item
# name is the condition value (e.g. AUX_CLOUD with OVERCAST=True).
_CONDITION_PROPERTIES: dict[str, str] = {
    "AUX_CLOUD": "cloud",
    "AUX_HUMIDITY": "humidity",
    "AUX_RAIN": "rain",
    "AUX_SKY": "sky",
    "AUX_WIND": "wind",
}


def _active_switch(prop: IndigoProperty) -> str | None:
    """Return the name of the active (True) switch item, or None."""
    for item_name, item in prop.items.items():
        if item.value is True:
            return item_name
    return None


@sk.declare_device(type=StandardWeather)
class IndigoWeather(IndigoDevice):
    """INDIGO AUX weather device.

    Subscribes to weather properties on the configured INDIGO device:
    - AUX_WEATHER (NumberVector) -> BasicWeather + IndigoWeatherStatus
    - AUX_CLOUD, AUX_HUMIDITY, AUX_RAIN, AUX_SKY, AUX_WIND (SwitchVectors)
      -> WeatherConditions
    """

    config: IndigoWeatherConfig
    device_name = "Weather"

    def __init__(self, config):
        super().__init__(config)
        self._conditions = WeatherConditions()
        self._conditions_dirty = False
        self._conditions_debounce: asyncio.Task | None = None

    @sk.on_attach
    async def entity_init(self):
        device = sk.device()

        # Restore state
        try:
            self.state = await device.kv_get_model(IndigoWeatherState)
            logger.debug(f"restored state for {device.entity}")
        except Exception:
            logger.warning(f"No saved state for {device.entity}")
            self.state = IndigoWeatherState()

        # Initialize the weather
        await self.weather_init(Init())

    @sk.on_detach
    async def entity_deinit(self):
        # Deinitialize the weather
        await self.weather_deinit(Deinit())

        # Clean up, disconnect
        await self.disconnect_from_server()
        await sk.device().publish(Connected(is_connected=False))
        await sk.device().kv_put_model(self.state)

    @sk.command_handler
    async def weather_init(self, cmd: Init):
        # Connect WebSocket to INDIGO server
        await self.connect_to_server()

        # Register callback for numeric weather data
        self.client.on_property(self.config.indigo_device, "AUX_WEATHER")(self._on_weather_update)

        # Register callbacks for each condition property
        for prop_name in _CONDITION_PROPERTIES:
            self.client.on_property(self.config.indigo_device, prop_name)(
                self._on_condition_update
            )

        # Track CONNECTION property to maintain device_connected state
        self.client.on_property(self.config.indigo_device, "CONNECTION")(
            self._on_connection_update
        )

        # Wait for initial weather property to appear
        try:
            async with asyncio.timeout(self.config.timeout):
                while not self.client.get_property(self.config.indigo_device, "AUX_WEATHER"):
                    await asyncio.sleep(0.25)
        except TimeoutError:
            logger.warning(
                f"AUX_WEATHER property not received within {self.config.timeout}s; "
                "will publish when it arrives"
            )

    @sk.command_handler
    async def weather_deinit(self, cmd: Deinit):
        pass

    async def _on_connection_update(self, prop: IndigoProperty, msg_type: str):
        """Track device connection state from INDIGO CONNECTION property."""
        connected_item = prop.item_value("CONNECTED", False)
        self.device_connected = bool(connected_item)
        await sk.device().publish(Connected(is_connected=self.device_connected))

    async def _on_weather_update(self, prop: IndigoProperty, msg_type: str):
        """Callback for AUX_WEATHER — numeric sensor readings."""
        device = sk.device()

        weather_kwargs: dict[str, float | None] = {}
        all_items: dict[str, float | str | None] = {}

        for item_name, item in prop.items.items():
            value = item.value
            all_items[item_name] = value

            if item_name in _WEATHER_FIELD_MAP and value is not None:
                try:
                    weather_kwargs[_WEATHER_FIELD_MAP[item_name]] = float(value)
                except (ValueError, TypeError):
                    pass

        if weather_kwargs:
            await device.publish(BasicWeather(**weather_kwargs))

        await device.publish(
            IndigoWeatherStatus(
                property_state=prop.state,
                items=all_items,
            )
        )

    async def _on_condition_update(self, prop: IndigoProperty, msg_type: str):
        """Callback for AUX_CLOUD, AUX_RAIN, etc. — categorical conditions.

        Condition properties arrive as a burst (one per category). We debounce
        by buffering updates and publishing once after 0.5s of quiet.
        """
        field = _CONDITION_PROPERTIES.get(prop.name)
        if not field:
            return

        active = _active_switch(prop)
        if active is not None:
            value = active.replace("_", " ").title()
            setattr(self._conditions, field, value)
            self._conditions_dirty = True

            # Reset the debounce timer
            if self._conditions_debounce is not None:
                self._conditions_debounce.cancel()
            self._conditions_debounce = asyncio.create_task(self._flush_conditions())

    async def _flush_conditions(self):
        """Wait for the burst to settle, then publish once."""
        await asyncio.sleep(0.5)
        if self._conditions_dirty:
            self._conditions_dirty = False
            await sk.device().publish(self._conditions)


class IndigoWeatherConfig(IndigoDeviceConfig[IndigoWeather]):
    device_type: Literal["weather"] = "weather"
    timeout: float = 30.0

    @override
    def create_device(self):
        return IndigoWeather(self)


class IndigoWeatherState(IndigoDeviceState):
    device_type: Literal["weather"] = "weather"
