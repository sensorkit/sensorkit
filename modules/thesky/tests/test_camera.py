import pytest

from sensorkit.thesky.camera import TheSkyCamera, TheSkyCameraConfig
from sensorkit.thesky.device import send_thesky_script, parse_thesky_response
from sensorkit.std.instrument import ConfigureCameraCooler, ConfigureCameraSensor, Binning, CameraSensorTemperature
from sensorkit.models.devices import TemperatureUnit


@pytest.fixture
def camera(simulator):
    host, port = simulator
    config = TheSkyCameraConfig(
        device_type="camera",
        host=host,
        port=port,
        temperature=-10,
        timeout=5.0,
        status_frequency=0.1,
    )
    return config.create_device()


@pytest.mark.asyncio
async def test_camera_connect(camera):
    from sensorkit.models.devices import Connected
    import sensorkit.api as sk

    await camera.camera_connect(sk.Connect())
    assert camera.device_connected is True


@pytest.mark.asyncio
async def test_camera_disconnect(camera):
    import sensorkit.api as sk

    await camera.camera_connect(sk.Connect())
    await camera.camera_disconnect(sk.Disconnect())
    assert camera.device_connected is False


@pytest.mark.asyncio
async def test_set_temperature(camera):
    import sensorkit.api as sk

    await camera.camera_connect(sk.Connect())
    await camera.camera_set_temperature(
        ConfigureCameraCooler(
            enable=True,
            setpoint=CameraSensorTemperature(temperature=-20, units=TemperatureUnit.CELSIUS),
        )
    )
    # Verify by querying the setpoint
    resp = await camera.execute("ccdsoftCamera.TemperatureSetPoint;")
    assert float(resp) == -20.0


@pytest.mark.asyncio
async def test_set_binning(camera):
    import sensorkit.api as sk

    await camera.camera_connect(sk.Connect())
    await camera.camera_set_binning(
        ConfigureCameraSensor(binning=Binning(x=2, y=2))
    )
    resp = await camera.execute(
        """
        var Out;
        Out = [ccdsoftCamera.BinX, ccdsoftCamera.BinY];
        """
    )
    bx, by = [int(x) for x in resp.split(",")]
    assert bx == 2
    assert by == 2


@pytest.mark.asyncio
async def test_set_binning_none(camera):
    """ConfigureCameraSensor with no binning should be a no-op."""
    import sensorkit.api as sk

    await camera.camera_connect(sk.Connect())
    await camera.camera_set_binning(ConfigureCameraSensor())
