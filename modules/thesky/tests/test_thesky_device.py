# SPDX-License-Identifier: Apache-2.0
import asyncio
from asyncio import StreamReader, StreamWriter

import pytest

from sensorkit.thesky.device import (
    SCRIPT_FOOTER,
    SCRIPT_HEADER,
    CommandNotSupportedError,
    ProcessAbortedError,
    TheSkyDevice,
    TheSkyDeviceConfig,
    TheSkyError,
    parse_thesky_response,
    send_thesky_script,
)


@pytest.mark.asyncio
async def test_send_thesky_script():
    command = b"var x = 1;\n"
    response = b"OK|No error. Error = 0.\n"
    parse_result = asyncio.get_running_loop().create_future()

    async def handle_connection(reader: StreamReader, writer: StreamWriter):
        try:
            data = b""
            async with asyncio.timeout(1.0):
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    data += chunk
                    if SCRIPT_FOOTER in data:
                        break

            assert data.startswith(SCRIPT_HEADER)
            assert SCRIPT_FOOTER in data
            user_script = data.removeprefix(SCRIPT_HEADER).split(SCRIPT_FOOTER)[0]
            assert user_script == command

            parse_result.set_result(True)
        except Exception as e:
            parse_result.set_exception(e)
        else:
            writer.write(response)
        finally:
            writer.write_eof()
            await writer.drain()
            writer.close()
            await writer.wait_closed()

    async with asyncio.timeout(2.0):
        server = await asyncio.start_server(handle_connection, host="127.0.0.1")
        host, port = server.sockets[0].getsockname()

        try:
            result = await send_thesky_script(host, port, command)
            await parse_result
            assert result == response
        finally:
            server.close()


def test_parse_thesky_response():
    # Success response.
    result = "  RESULT STRING  "
    no_error = "No error. Error = 0."
    payload = f"{result}|{no_error}\n".encode()

    parsed = parse_thesky_response(payload)
    assert parsed == result

    # Outer error response.
    error_message = "Error message"
    error_code = 4
    error_payload = f"|{error_message}. Error = {error_code}.\n".encode()

    with pytest.raises(TheSkyError) as e:
        parse_thesky_response(error_payload)

    assert e.value.code == error_code
    assert e.value.args[0] == error_message

    # Inner error response.
    error_message = "Process aborted"
    error_code = 212
    error_payload = f"TypeError: {error_message}. Error = {error_code}.|{no_error}\n".encode()

    with pytest.raises(TheSkyError) as e:
        parse_thesky_response(error_payload)

    assert isinstance(e.value, ProcessAbortedError)
    assert e.value.code == error_code
    assert e.value.args[0] == error_message

    # Unsupported command (e.g. FindHome on a sim mount) maps to its typed error.
    error_message = "This command is not supported by the selected device"
    error_code = 228
    error_payload = f"TypeError: {error_message}. Error = {error_code}.|{no_error}\n".encode()

    with pytest.raises(TheSkyError) as e:
        parse_thesky_response(error_payload)

    assert isinstance(e.value, CommandNotSupportedError)
    assert e.value.code == error_code
    assert e.value.args[0] == error_message


def test_thesky_device():
    cfg = TheSkyDeviceConfig(device_type=None, host="localhost", port=3040)
    device = cfg.create_device()
    assert isinstance(device, TheSkyDevice)
    assert device.config is cfg
