import pytest

from sensorkit.udl.models import ResponseStatus, UDLAPIConfig, UDLConfig, UDLReferenceFrame


class TestResponseStatus:
    def test_values(self):
        assert ResponseStatus.ACCEPTED == "ACCEPTED"
        assert ResponseStatus.COLLECTED == "COLLECTED"
        assert ResponseStatus.FAILED == "FAILED"
        assert ResponseStatus.CANCELLED == "CANCELLED"
        assert ResponseStatus.REJECTED == "REJECTED"


class TestUDLReferenceFrame:
    def test_j2000_to_gcrf(self):
        from sensorkit.astro.common import ReferenceFrame
        assert UDLReferenceFrame.J2000.to_sensorkit_frame() == ReferenceFrame.GCRF

    def test_icrf_to_gcrf(self):
        from sensorkit.astro.common import ReferenceFrame
        assert UDLReferenceFrame.ICRF.to_sensorkit_frame() == ReferenceFrame.GCRF

    def test_teme_to_teme(self):
        from sensorkit.astro.common import ReferenceFrame
        assert UDLReferenceFrame.TEME.to_sensorkit_frame() == ReferenceFrame.TEME

    def test_efg_tdr_to_itrf(self):
        from sensorkit.astro.common import ReferenceFrame
        assert UDLReferenceFrame.EFG_TDR.to_sensorkit_frame() == ReferenceFrame.ITRF


class TestUDLAPIConfig:
    def test_required_fields(self):
        config = UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        )
        assert config.id_sensor == "SENSOR-01"
        assert config.source == "TEST_SOURCE"
        assert config.use_certs is False
        assert config.timeout == 60.0

    def test_cert_auth_config(self):
        config = UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="MACHINA",
            use_certs=True,
            client_cert="/path/to/cert.pem",
            client_key="/path/to/key.pem",
            client_verify=False,
            base_url="https://machina.example.com",
        )
        assert config.use_certs is True
        assert config.client_cert == "/path/to/cert.pem"
        assert config.base_url == "https://machina.example.com"

    def test_env_file_config(self):
        config = UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
            env_file="/path/to/.env",
        )
        assert config.env_file == "/path/to/.env"

    def test_env_file_default(self):
        config = UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        )
        assert config.env_file == ".env"


class TestUDLConfig:
    def test_defaults(self):
        config = UDLConfig(
            entity="udl_program",
            controller="controller1",
            api=UDLAPIConfig(
                id_sensor="SENSOR-01",
                source="TEST_SOURCE",
            ),
        )
        assert config.poll_frequency == 10.0
        assert config.end_time_deadband_s == 0.0
        assert config.skyimagery_save_path is None
