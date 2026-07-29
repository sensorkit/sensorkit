# SPDX-License-Identifier: Apache-2.0
import asyncio

import pytest
import pytest_asyncio

from sensorkit.thesky.device import SCRIPT_FOOTER, SCRIPT_HEADER
from sensorkit.thesky.simulator import SimulationState, read_template, run_thesky_script


@pytest.fixture(autouse=True)
def _clear_template_cache():
    """Clear the cached simulator template so changes to simulator.js are picked up."""
    read_template.cache_clear()


@pytest.fixture(autouse=True)
def _autouse_device_context(device_impl):
    """All tests in this suite may access an active `DeviceImpl` via `sk.device()`."""


@pytest_asyncio.fixture
async def simulator():
    """Start a TheSky simulator on a random port and yield (host, port).

    Unlike the standalone TheSkySimulator (which uses reader.read() and expects
    the client to close the connection first), this test fixture reads until it
    finds the SCRIPT_FOOTER delimiter, then immediately responds. This avoids
    a deadlock with send_thesky_script, which waits for a response before closing.
    """
    state = SimulationState(
        ccdsoftCamera={},
        sky6RASCOMTele={},
        sky6Dome={},
        OpticalTubeAssembly={},
        WeatherUtil={},
        sky6StarChart={},
        Raven3={},
    )

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        nonlocal state
        data = b""
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            data += chunk
            if SCRIPT_FOOTER in data:
                break

        response = None
        if data.startswith(SCRIPT_HEADER) and SCRIPT_FOOTER in data:
            # Extract the user script and wrap single bare expressions so Out is set.
            user_script = data.removeprefix(SCRIPT_HEADER).split(SCRIPT_FOOTER)[0]
            stripped = user_script.strip()
            # A single expression with no semicolons in the middle (just one trailing)
            # and no assignments to Out — wrap it as Out = <expr>;
            inner = stripped.rstrip(b";").strip()
            if (
                b"Out" not in stripped
                and b";" not in inner
                and b"\n" not in inner
            ):
                data = SCRIPT_HEADER + b"Out = " + inner + b";" + SCRIPT_FOOTER
            js = data
            try:
                simulation = await asyncio.to_thread(run_thesky_script, js, state)
            except Exception as e:
                # Match TheSky's real script-error format (see simulator.py): the
                # trailing "|No error. Error = 0." is what makes it parseable.
                response = f"TypeError: {str(e)}. Error = 250.|No error. Error = 0."
            else:
                result = simulation.get("result")

                def fmt(v):
                    if isinstance(v, bool):
                        return "true" if v else "false"
                    if isinstance(v, float):
                        # Preserve integer-valued floats as ints (e.g. 1.0 -> "1")
                        return str(int(v)) if v == int(v) else str(v)
                    if isinstance(v, int):
                        return str(v)
                    if v is None:
                        return "null"
                    return str(v)

                match result:
                    case list():
                        response = ",".join(fmt(part) for part in result)
                    case _:
                        response = fmt(result)
                state = simulation["state"]
                response += "|No error. Error = 0."

        if response is not None:
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1")
    host, port = server.sockets[0].getsockname()

    try:
        yield host, port
    finally:
        server.close()
        await server.wait_closed()
