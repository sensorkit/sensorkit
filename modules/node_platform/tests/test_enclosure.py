# SPDX-License-Identifier: Apache-2.0
"""Tests for Node Platform enclosure device."""

import asyncio

import ourskyai_node_platform_api as osapi
import pytest

from .fakes import (
    FakeNodePlatformAPI,
    make_enclosure_status,
    make_operation_status,
    make_safety_status,
)
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.enclosure import (
    NodePlatformEnclosure,
    NodePlatformEnclosureConfig,
    NodePlatformEnclosureState,
)
from sensorkit.std import Deinit, Home, Init, Stop
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure

Shutter = osapi.EnclosureShutterState


def install_shutter(api: FakeNodePlatformAPI, state: Shutter) -> dict:
    """Install a mutable shutter simulation. Open/close API calls mutate its state."""
    shutter = {"state": state}

    api.set_response(
        "v2_get_enclosure_status",
        lambda *a, **k: make_enclosure_status(state=shutter["state"]),
    )

    def _close(*a, **k):
        shutter["state"] = Shutter.CLOSED

    def _open(*a, **k):
        shutter["state"] = Shutter.OPENED

    api.set_response("v1_close_enclosure_shutters", _close)
    api.set_response("v1_open_enclosure_shutters", _open)
    return shutter


def install_safety(api: FakeNodePlatformAPI, *, is_safe: bool, autoclose: dict | None = None):
    """Install a safety-status response. If unsafe and *autoclose* is given, simulate the
    Platform's assisted-mode auto-close by flipping that shutter to CLOSED when polled."""

    def _safety(*a, **k):
        if not is_safe and autoclose is not None:
            autoclose["state"] = Shutter.CLOSED
        return make_safety_status(is_safe=is_safe)

    api.set_response("v1_get_safety_status", _safety)


def install_mode(api: FakeNodePlatformAPI, mode: str):
    """Install the live operation-mode response queried by the enclosure ('ASSISTED'/'MANUAL')."""
    api.set_response(
        "v1_get_system_operation_status", lambda *a, **k: make_operation_status(mode=mode)
    )


@pytest.fixture
def api():
    return FakeNodePlatformAPI()


@pytest.fixture
def enclosure(api):
    config = NodePlatformEnclosureConfig(
        device_type="enclosure",
        host="localhost",
        port=9080,
        operation_mode="manual",
    )
    enc = NodePlatformEnclosure(config)
    enc._api = api
    # A normally-operating enclosure has already been homed; open now homes first if not.
    enc.state = NodePlatformEnclosureState(has_been_homed=True)
    enc._shutter_lock = asyncio.Lock()
    enc._home_lock = asyncio.Lock()
    enc.device_connected = True
    return enc


def _config(operation_mode: str, **kwargs):
    return NodePlatformEnclosureConfig(
        device_type="enclosure", host="localhost", operation_mode=operation_mode, **kwargs
    )


class TestEnclosureConfig:
    def test_defaults(self):
        config = NodePlatformEnclosureConfig(device_type="enclosure", host="localhost")
        assert config.device_type == "enclosure"
        assert config.timeout == 120.0
        assert config.operation_mode == "assisted"

    def test_create_device(self):
        config = NodePlatformEnclosureConfig(device_type="enclosure", host="localhost")
        device = config.create_device()
        assert isinstance(device, NodePlatformEnclosure)


class TestEnclosureInit:
    @pytest.mark.asyncio
    async def test_attach_syncs_with_mount(self, enclosure, api):
        await enclosure._initialize()

        assert len(api.find_calls("v1_sync_enclosure_rotator_with_mount")) == 1
        assert len(api.find_calls("v1_sync_enclosure_window_with_mount")) == 1
        assert len(api.find_calls("v1_home_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_init_homes_if_needed(self, enclosure, api):
        enclosure.state.has_been_homed = False

        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = Shutter.CLOSED

        await enclosure.enclosure_init(Init())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 1
        assert enclosure.state.has_been_homed is True

    @pytest.mark.asyncio
    async def test_init_skips_home_if_already_homed(self, enclosure, api):
        enclosure.state.has_been_homed = True

        await enclosure.enclosure_init(Init())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_home_rejects_non_closed_terminal_state(self, enclosure, api):
        """A halted or faulted calibration must not be recorded as a successful home."""
        enclosure.state.has_been_homed = False

        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = Shutter.HALTED

        with pytest.raises(RuntimeError, match="HALTED"):
            await enclosure.enclosure_init(Init())

        assert enclosure.state.has_been_homed is False

    @pytest.mark.asyncio
    async def test_deinit_turns_off_fans(self, enclosure, api):
        enclosure.config = _config("manual", fans=True)

        await enclosure.enclosure_deinit(Deinit())

        assert len(api.find_calls("v1_turn_off_enclosure_fans")) == 1

    @pytest.mark.asyncio
    async def test_deinit_does_not_close(self, enclosure, api):
        """The sensor issues CloseEnclosure itself; Deinit must not abort or duplicate it."""
        install_shutter(api, Shutter.OPENED)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_deinit(Deinit())

        assert len(api.find_calls("v1_close_enclosure_shutters")) == 0
        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 0


class TestEnclosureOpen:
    @pytest.mark.asyncio
    async def test_open_in_manual_mode(self, enclosure, api):
        enclosure.config = _config("manual")
        install_shutter(api, Shutter.CLOSED)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_enable_manual_operation")) == 0

    @pytest.mark.asyncio
    async def test_open_in_assisted_mode_uses_manual_flip(self, enclosure, api):
        enclosure.config = _config("assisted")
        install_shutter(api, Shutter.CLOSED)
        install_mode(api, "ASSISTED")

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_enable_manual_operation")) == 1
        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_enable_assisted_operation")) == 1

    @pytest.mark.asyncio
    async def test_open_homes_first_if_never_homed(self, enclosure, api):
        """Shutter position is unknown before a home, so an open must home first.

        This is what makes a concurrent Init and OpenEnclosure order-independent: whichever
        arrives first, the home happens before the shutters open.
        """
        enclosure.config = _config("manual")
        enclosure.state.has_been_homed = False
        shutter = install_shutter(api, Shutter.CLOSED)
        install_mode(api, "MANUAL")
        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = Shutter.CLOSED

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1
        assert enclosure.state.has_been_homed is True
        assert shutter["state"] is Shutter.OPENED

    @pytest.mark.asyncio
    async def test_open_skips_home_when_already_homed(self, enclosure, api):
        enclosure.config = _config("manual")
        install_shutter(api, Shutter.CLOSED)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 0
        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1

    @pytest.mark.asyncio
    async def test_home_is_allowed_while_open(self, enclosure, api):
        """Homing an open enclosure is a legitimate operator action, not a conflict."""
        install_shutter(api, Shutter.OPENED)
        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = Shutter.CLOSED

        await enclosure.enclosure_home(Home())

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 1
        assert enclosure.state.has_been_homed is True

    @pytest.mark.asyncio
    async def test_open_halts_if_closing(self, enclosure, api):
        """An open while the shutter is closing halts (reverses) before opening."""
        enclosure.config = _config("manual")
        install_shutter(api, Shutter.MOVING_CLOSE)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_open(OpenEnclosure())

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_open_enclosure_shutters")) == 1

    @pytest.mark.asyncio
    async def test_open_requires_connected(self, enclosure):
        enclosure.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await enclosure.enclosure_open(OpenEnclosure())


class TestEnclosureClose:
    @pytest.mark.asyncio
    async def test_close_in_manual_mode(self, enclosure, api):
        enclosure.config = _config("manual")
        install_shutter(api, Shutter.OPENED)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_close_enclosure_shutters")) == 1
        # Manual mode never touches operation mode.
        assert len(api.find_calls("v1_enable_manual_operation")) == 0
        assert len(api.find_calls("v1_enable_assisted_operation")) == 0
        # Safety is irrelevant in manual mode and must not be queried.
        assert len(api.find_calls("v1_get_safety_status")) == 0

    @pytest.mark.asyncio
    async def test_close_assisted_safe_uses_manual_flip(self, enclosure, api):
        """Safe/scheduled shutdown: Platform won't auto-close, so we take MANUAL control."""
        enclosure.config = _config("assisted")
        install_shutter(api, Shutter.OPENED)
        install_mode(api, "ASSISTED")
        install_safety(api, is_safe=True)

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_enable_manual_operation")) == 1
        assert len(api.find_calls("v1_close_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_enable_assisted_operation")) == 1

    @pytest.mark.asyncio
    async def test_close_assisted_unsafe_defers_to_platform(self, enclosure, api):
        """Unsafe: the Platform closes on its own — we defer and never seize MANUAL mode."""
        enclosure.config = _config("assisted")
        shutter = install_shutter(api, Shutter.OPENED)
        install_mode(api, "ASSISTED")
        install_safety(api, is_safe=False, autoclose=shutter)

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_close_enclosure_shutters")) == 0
        assert len(api.find_calls("v1_enable_manual_operation")) == 0
        assert len(api.find_calls("v1_enable_assisted_operation")) == 0

    @pytest.mark.asyncio
    async def test_close_assisted_unsafe_times_out_if_platform_stalls(self, enclosure, api):
        """Unsafe: we defer to the Platform and never seize MANUAL — even if it stalls."""
        enclosure.config = _config("assisted", timeout=0.3)
        install_shutter(api, Shutter.OPENED)
        install_mode(api, "ASSISTED")
        # Unsafe, but no autoclose side effect -> Platform never closes on its own.
        install_safety(api, is_safe=False)

        with pytest.raises(TimeoutError):
            await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_enable_manual_operation")) == 0
        assert len(api.find_calls("v1_close_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_close_halts_if_opening(self, enclosure, api):
        """A close while the shutter is opening halts (reverses) before closing."""
        enclosure.config = _config("manual")
        install_shutter(api, Shutter.MOVING_OPEN)
        install_mode(api, "MANUAL")

        await enclosure.enclosure_close(CloseEnclosure())

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_close_enclosure_shutters")) == 1

    @pytest.mark.asyncio
    async def test_close_requires_connected(self, enclosure):
        enclosure.device_connected = False

        with pytest.raises(DeviceConnectionError):
            await enclosure.enclosure_close(CloseEnclosure())


class TestEnclosureStop:
    @pytest.mark.asyncio
    async def test_stop(self, enclosure, api):
        await enclosure.enclosure_stop(Stop())

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_halt_enclosure_window")) == 1
