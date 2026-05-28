import pytest

from sensorkit.std import Connect, Disconnect
from sensorkit.std.optics import CloseMirrorCover, OpenMirrorCover
from sensorkit.thesky.ota import TheSkyOTAConfig, TheSkyOTAState


@pytest.fixture
def ota(simulator):
    host, port = simulator
    config = TheSkyOTAConfig(
        device_type="ota",
        host=host,
        port=port,
        timeout=5.0,
        status_frequency=0.1,
    )
    o = config.create_device()
    o.state = TheSkyOTAState()
    return o


@pytest.mark.asyncio
async def test_ota_connect(ota):
    await ota.ota_connect(Connect())
    assert ota.device_connected is True


@pytest.mark.asyncio
async def test_ota_disconnect(ota):
    await ota.ota_connect(Connect())
    await ota.ota_disconnect(Disconnect())
    assert ota.device_connected is False


@pytest.mark.asyncio
async def test_ota_open(ota):
    await ota.ota_connect(Connect())
    await ota.ota_open(OpenMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "1"


@pytest.mark.asyncio
async def test_ota_close(ota):
    await ota.ota_connect(Connect())
    # Open first, then close
    await ota.ota_open(OpenMirrorCover())
    await ota.ota_close(CloseMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "0"


@pytest.mark.asyncio
async def test_ota_open_when_already_open(ota):
    """Opening when already open should succeed (idempotent)."""
    await ota.ota_connect(Connect())
    await ota.ota_open(OpenMirrorCover())
    # Call again - should still succeed
    await ota.ota_open(OpenMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "1"


@pytest.mark.asyncio
async def test_ota_close_when_already_closed(ota):
    """Closing when already closed should succeed (idempotent)."""
    await ota.ota_connect(Connect())
    await ota.ota_close(CloseMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "0"
