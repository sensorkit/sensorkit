"""Tests for otto's UDL SkyImagery publisher."""

import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from sensorkit.otto.models import UDLPublishConfig
from sensorkit.otto.publishers import UDLPublisher

# A path that never exists: dotenv_values() quietly returns {}, so credentials
# resolve purely from the (test-controlled) process environment.
NO_ENV_FILE = "/nonexistent/.env"


def make_config(**overrides):
    defaults = dict(id_sensor="TEST_SENSOR", source="TEST")
    defaults.update(overrides)
    return UDLPublishConfig(**defaults)


def make_publisher(config=None, **kwargs):
    kwargs.setdefault("env_file", NO_ENV_FILE)
    return UDLPublisher(config or make_config(), **kwargs)


@pytest.fixture
def no_udl_env(monkeypatch):
    """Ensure the process environment carries no UDL credentials."""
    monkeypatch.delenv("UDL_USERNAME", raising=False)
    monkeypatch.delenv("UDL_PASSWORD", raising=False)


@pytest.fixture
def udl_env(monkeypatch):
    """Provide UDL credentials via the process environment."""
    monkeypatch.setenv("UDL_USERNAME", "user")
    monkeypatch.setenv("UDL_PASSWORD", "pass")


SITE = SimpleNamespace(latitude_degrees=36.06, longitude_degrees=-115.14, altitude_km=0.05)


class TestCredentials:
    def test_env_file_wins_over_process_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("UDL_USERNAME", "env-user")
        monkeypatch.setenv("UDL_PASSWORD", "env-pass")
        env_file = tmp_path / ".env"
        env_file.write_text("UDL_USERNAME=file-user\nUDL_PASSWORD=file-pass\n")

        pub = UDLPublisher(make_config(), env_file=str(env_file))

        assert pub._sdk.username == "file-user"

    def test_falls_back_to_process_env(self, udl_env):
        pub = make_publisher()
        assert pub._sdk.username == "user"

    def test_partial_credentials_rejected(self, no_udl_env, monkeypatch):
        monkeypatch.setenv("UDL_USERNAME", "user-only")
        with pytest.raises(RuntimeError, match="both"):
            make_publisher()

    def test_no_credentials_is_unauthenticated(self, no_udl_env):
        pub = make_publisher()
        assert pub._sdk.username is None
        assert pub._sdk.auth_headers == {}


class TestImageryBaseURL:
    def test_default_is_production_imagery_host(self, no_udl_env):
        pub = make_publisher()
        assert pub._imagery_base_url() == "https://imagery.unifieddatalibrary.com"

    def test_test_host_maps_to_imagery_test(self, no_udl_env):
        pub = make_publisher(make_config(base_url="https://test.unifieddatalibrary.com"))
        assert pub._imagery_base_url() == "https://imagery-test.unifieddatalibrary.com"

    def test_production_host_maps_to_imagery(self, no_udl_env):
        pub = make_publisher(make_config(base_url="https://unifieddatalibrary.com/"))
        assert pub._imagery_base_url() == "https://imagery.unifieddatalibrary.com"

    def test_custom_host_serves_filedrop_in_place(self, no_udl_env):
        pub = make_publisher(make_config(base_url="https://udl-compliant.example:8443"))
        assert pub._imagery_base_url() == "https://udl-compliant.example:8443"

    def test_sdk_client_bound_to_imagery_host(self, no_udl_env):
        pub = make_publisher()
        assert str(pub._sdk.base_url).startswith("https://imagery.unifieddatalibrary.com")


class TestBuildMetadata:
    def test_required_fields(self, no_udl_env):
        pub = make_publisher(frame_count=3, site=SITE)
        context = {
            "task_id": "abc123",
            "frame_num": 1,
            "image_width": 1024,
            "image_height": 1024,
        }

        md = pub._build_metadata(context, b"\x00" * 10, "frame.fits")

        assert md["classificationMarking"] == "U"
        assert md["idSensor"] == "TEST_SENSOR"
        assert md["source"] == "TEST"
        assert md["dataMode"] == "TEST"
        assert md["imageType"] == "FITS"
        assert md["filename"] == "frame.fits"
        assert md["filesize"] == 10
        assert md["sequenceId"] == 2  # frame_num + 1 (UDL requires >= 1)
        assert md["imageSetLength"] == 3
        assert md["imageSetId"] == "abc123"
        assert md["senlat"] == SITE.latitude_degrees
        # satNo not in context -> omitted entirely
        assert "satNo" not in md

    def test_single_frame_set_has_no_image_set_id(self, no_udl_env):
        pub = make_publisher(frame_count=1)
        md = pub._build_metadata({"task_id": "abc123"}, b"", "f.fits")
        assert "imageSetId" not in md
        assert md["imageSetLength"] == 1
        assert md["sequenceId"] == 1

    def test_exposure_window_from_context(self, no_udl_env):
        pub = make_publisher()
        md = pub._build_metadata(
            {"date_obs": "2026-07-04T01:02:03+00:00", "exptime": 5}, b"", "f.fits"
        )
        assert md["expStartTime"] == "2026-07-04T01:02:03.000000Z"
        assert md["expEndTime"] == "2026-07-04T01:02:08.000000Z"


class TestPublish:
    @pytest.mark.asyncio
    async def test_posts_zip_with_metadata_and_image(self, udl_env):
        pub = make_publisher(frame_count=2)
        pub._sdk._client.post = AsyncMock(
            return_value=SimpleNamespace(status_code=200, text="")
        )

        await pub.publish({"task_id": "t1", "frame_num": 0}, b"FITSDATA")

        (url,), kwargs = pub._sdk._client.post.call_args
        assert url == "/filedrop/udl-skyimagery"  # relative to the imagery base_url
        assert kwargs["headers"]["Content-Type"] == "application/zip"
        assert kwargs["headers"]["Authorization"].startswith("Basic ")

        with zipfile.ZipFile(io.BytesIO(kwargs["content"])) as zf:
            names = sorted(zf.namelist())
            fits_name = next(n for n in names if n.endswith(".fits"))
            json_name = next(n for n in names if n.endswith("_skyimagery.json"))
            assert zf.read(fits_name) == b"FITSDATA"
            md = json.loads(zf.read(json_name))
            assert md["idSensor"] == "TEST_SENSOR"
            assert md["imageSetId"] == "t1"

    @pytest.mark.asyncio
    async def test_unauthenticated_post_has_no_auth_header(self, no_udl_env):
        pub = make_publisher()
        pub._sdk._client.post = AsyncMock(
            return_value=SimpleNamespace(status_code=200, text="")
        )

        await pub.publish({"frame_num": 0}, b"x")

        _, kwargs = pub._sdk._client.post.call_args
        assert "Authorization" not in kwargs["headers"]

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self, udl_env):
        pub = make_publisher()
        pub._sdk._client.post = AsyncMock(
            return_value=SimpleNamespace(status_code=401, text="unauthorized")
        )

        with pytest.raises(RuntimeError, match="HTTP 401"):
            await pub.publish({"frame_num": 0}, b"x")

    @pytest.mark.asyncio
    async def test_close_closes_sdk_client(self, no_udl_env):
        pub = make_publisher()
        await pub.close()
        assert pub._sdk.is_closed()
