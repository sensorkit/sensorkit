import pytest

from sensorkit.thesky.ota import TheSkyOTA, TheSkyOTAConfig, TheSkyOTAState
from sensorkit.std.optics import OpenMirrorCover, CloseMirrorCover


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
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    assert ota.device_connected is True


@pytest.mark.asyncio
async def test_ota_disconnect(ota):
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    await ota.ota_disconnect(sk.Disconnect())
    assert ota.device_connected is False


@pytest.mark.asyncio
async def test_ota_open(ota):
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    await ota.ota_open(OpenMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "1"


@pytest.mark.asyncio
async def test_ota_close(ota):
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    # Open first, then close
    await ota.ota_open(OpenMirrorCover())
    ota.state.mirror_cover_status = "open"
    await ota.ota_close(CloseMirrorCover())

    resp = await ota.execute("OpticalTubeAssembly.mirrorCoverState;")
    assert resp.strip() == "0"


@pytest.mark.asyncio
async def test_ota_open_skips_if_already_open(ota):
    """Should return immediately if already open."""
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    ota.state.mirror_cover_status = "open"
    await ota.ota_open(OpenMirrorCover())


@pytest.mark.asyncio
async def test_ota_close_skips_if_already_closed(ota):
    """Should return immediately if already closed."""
    import sensorkit.api as sk

    await ota.ota_connect(sk.Connect())
    ota.state.mirror_cover_status = "closed"
    await ota.ota_close(CloseMirrorCover())
