"""monitor_indigo.py — Print live data published by the indigo_service.

Connects to the running SensorKit backend and monitors all keywords
published by the INDIGO weather device.

Prerequisites:
    1. NATS is running
    2. indigo_service is running

Usage:
    python modules/indigo/tests/monitor_indigo.py
"""

import asyncio

DEVICE_NAME = "weather1"


async def main():
    from sensorkit.api.bootstrap import connect
    from sensorkit.models.devices import Connected
    from sensorkit.std.weather import BasicWeather
    from sensorkit.indigo.weather import IndigoWeatherStatus, WeatherConditions

    kit = await connect()
    device = kit.device(DEVICE_NAME)

    print(f"Monitoring '{DEVICE_NAME}' — Ctrl+C to quit\n")

    async def watch(keyword_cls, label):
        monitor = await device.monitor(keyword_cls)
        async for _, value in monitor:
            print(f"[{label}] {value.model_dump(exclude_none=True)}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(watch(Connected, "Connected"))
        tg.create_task(watch(BasicWeather, "BasicWeather"))
        tg.create_task(watch(WeatherConditions, "Conditions"))
        tg.create_task(watch(IndigoWeatherStatus, "IndigoStatus"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDone.")
