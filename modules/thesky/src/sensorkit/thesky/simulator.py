import asyncio
import functools
import json
import pathlib
import time
from collections.abc import Sequence
from typing import Any, TypedDict, cast

import click
import dukpy
from loguru import logger

from sensorkit.thesky.device import SCRIPT_FOOTER, SCRIPT_HEADER

TEMPLATE_FILE = "simulator.js"
TEMPLATE_MARKER = b"//USER_SCRIPT"


class SimulationState(TypedDict):
    ccdsoftCamera: dict[str, Any]
    sky6RASCOMTele: dict[str, Any]


class Simulation(TypedDict):
    state: SimulationState
    events: Sequence[Any]
    runtime: float
    result: Any


@functools.cache
def read_template():
    with open(pathlib.Path(__file__).parent / TEMPLATE_FILE, "rb") as f:
        tmpl = f.read()

    # Split on the user script marker, or else raise ValueError.
    parts = tmpl.split(TEMPLATE_MARKER)

    if len(parts) != 2:
        raise ValueError("expected exactly one user script marker in template")

    return parts[0], parts[1]


def run_thesky_script(script: bytes, state: SimulationState = None):
    pre, post = read_template()
    return cast(
        Simulation,
        dukpy.evaljs(pre + script + post, State=state)
    )


class TheSkySimulator:
    """A simulator for the TheSky TCP interface."""

    def __init__(self, host: str = "127.0.0.1", port: int = 3040):
        self.host = host
        self.port = port
        self._state = SimulationState(ccdsoftCamera={}, sky6RASCOMTele={})

    @staticmethod
    def _simulate(js: bytes, state: SimulationState):
        try:
            simulation = run_thesky_script(js, state)
        except Exception as e:
            logger.exception("Error running script")
            response = f"TypeError: {str(e)}. Error = 250."
        else:
            # Simulate runtime, if any.
            if runtime := simulation["runtime"]:
                logger.debug(f"sleeping for simulated runtime {runtime}ms")
                time.sleep(runtime / 1000)

            # Construct the response.
            result = simulation["result"]

            match result:
                case Sequence():
                    response = ",".join(json.dumps(part) for part in result)
                case _:
                    response = json.dumps(result)

            # Return the new state.
            state = simulation["state"]

        return response, state

    async def _handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            js = await reader.read()
        except Exception:
            logger.exception("Error reading from client")
            raise

        response: str | None

        if not js.startswith(SCRIPT_HEADER) and js.endswith(SCRIPT_FOOTER):
            response = None
        else:
            response, state = await asyncio.to_thread(self._simulate, js, self._state)

            # Store the new state.
            self._state = state

            # Append outer success code.
            response += "|No error. Error = 0."

            logger.debug(f"""
                user script: {js.removeprefix(SCRIPT_HEADER).removesuffix(SCRIPT_FOOTER).decode()}
                response: {response}
                state: {self._state}""")

        if response is not None:
            try:
                writer.write(response.encode())
                await writer.drain()
            except Exception:
                logger.exception("Error writing to client")

        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            logger.exception("Error closing connection")

    async def run(self):
        # Make sure the template is cached.
        await asyncio.to_thread(read_template)

        # Start the server.
        server = await asyncio.start_server(self._handler, self.host, self.port)
        addr = server.sockets[0].getsockname()
        logger.info(f"Simulator listening on {addr}")

        async with server:
            await server.serve_forever()


@click.command()
@click.option("--host", default="127.0.0.1", help="Host address to bind to")
@click.option("--port", default=3040, help="Port to listen on", type=int)
def main(host, port):
    simulator = TheSkySimulator(host=host, port=port)
    asyncio.run(simulator.run())


if __name__ == "__main__":
    main()
