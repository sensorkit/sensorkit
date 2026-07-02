"""Tests for Node Platform enclosure device."""

import ourskyai_node_platform_api as osapi
import pytest
from conftest import (
    MockNodePlatformAPI,
    make_enclosure_status,
    make_operation_status,
    make_safety_status,
)

from sensorkit.models.devices import Stop
from sensorkit.node_platform.device import DeviceConnectionError
from sensorkit.node_platform.enclosure import (
    NodePlatformEnclosure,
    NodePlatformEnclosureConfig,
    NodePlatformEnclosureState,
)
from sensorkit.std.enclosure import CloseEnclosure, OpenEnclosure

Shutter = osapi.EnclosureShutterState


def install_shutter(api: MockNodePlatformAPI, state: Shutter) -> dict:
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


def install_safety(api: MockNodePlatformAPI, *, is_safe: bool, autoclose: dict | None = None):
    """Install a safety-status response. If unsafe and *autoclose* is given, simulate the
    Platform's assisted-mode auto-close by flipping that shutter to CLOSED when polled."""

    def _safety(*a, **k):
        if not is_safe and autoclose is not None:
            autoclose["state"] = Shutter.CLOSED
        return make_safety_status(is_safe=is_safe)

    api.set_response("v1_get_safety_status", _safety)


def install_mode(api: MockNodePlatformAPI, mode: str):
    """Install the live operation-mode response queried by the enclosure ('ASSISTED'/'MANUAL')."""
    api.set_response("v1_get_system_operation_status", lambda *a, **k: make_operation_status(mode=mode))


@pytest.fixture
def api():
    return MockNodePlatformAPI()


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
    enc.state = NodePlatformEnclosureState()
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
    async def test_init_homes_if_needed(self, enclosure, api):
        enclosure.state.has_been_homed = False

        api.set_response("v1_home_enclosure_shutters", None)
        enclosure.shutter_state = Shutter.CLOSED

        await enclosure._initialize()

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_sync_enclosure_rotator_with_mount")) == 1
        assert len(api.find_calls("v1_sync_enclosure_window_with_mount")) == 1

    @pytest.mark.asyncio
    async def test_init_skips_home_if_already_homed(self, enclosure, api):
        enclosure.state.has_been_homed = True

        await enclosure._initialize()

        assert len(api.find_calls("v1_home_enclosure_shutters")) == 0

    @pytest.mark.asyncio
    async def test_deinit_stops(self, enclosure, api):
        install_shutter(api, Shutter.OPENED)
        install_mode(api, "MANUAL")

        await enclosure._deinitialize()

        assert len(api.find_calls("v1_halt_enclosure_shutters")) == 1
        assert len(api.find_calls("v1_halt_enclosure_window")) == 1
        assert len(api.find_calls("v1_close_enclosure_shutters")) == 1


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
