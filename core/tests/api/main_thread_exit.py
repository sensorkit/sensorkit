# SPDX-License-Identifier: Apache-2.0
"""Raise SystemExit on the main event loop while a service runs.

The parent test checks the exit status and service teardown marker.
"""

import asyncio

from sensorkit.api.declarative import Service
from sensorkit.api.entrypoint import ServiceEntrypoint, run_services


def exit_main_loop():
    raise SystemExit(3)


async def main():
    main_loop = asyncio.get_running_loop()

    async def companion(svc: Service):
        await svc.start()

        # Stands in for a main loop callback, such as a signal handler, that exits.
        main_loop.call_soon_threadsafe(exit_main_loop)

        try:
            await svc.shutdown
        finally:
            print("companion torn down", flush=True)

    await run_services(
        {"companion": ServiceEntrypoint(func=companion, version="0.1.0")},
        trap_signals=False,
        shutdown_timeout=1.0,
    )


if __name__ == "__main__":
    asyncio.run(main())
