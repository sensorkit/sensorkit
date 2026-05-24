import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import ICRSTarget, StateVectorTarget, TLETarget
from sensorkit.udl.models import UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram

from conftest import MockCollectRequest


@pytest.fixture
def program():
    config = UDLConfig(
        entity="udl_program",
        controller="controller1",
        api=UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        ),
    )
    p = UDLProgram()
    p.config = config
    return p


class TestBuildTargetTLE:
    def test_tle_target(self, program):
        request = MockCollectRequest.with_tle(sat_no=25544)
        target = program._build_target(request)

        assert isinstance(target, TLETarget)
        assert target.tle.line0 == "0 25544"
        assert "25544" in target.tle.line1

    def test_tle_target_no_sat_no(self, program):
        request = MockCollectRequest.with_tle(sat_no=None, orig_object_id="99999")
        request.elset.sat_no = None
        target = program._build_target(request)

        assert isinstance(target, TLETarget)
        assert target.tle.line0 == "0 99999"


class TestBuildTargetStateVector:
    def test_state_vector_target(self, program):
        request = MockCollectRequest.with_state_vector()
        target = program._build_target(request)

        assert isinstance(target, StateVectorTarget)
        # Positions converted from km to m
        assert target.sv.r.x == 6778000.0
        assert target.sv.r.y == 0.0
        assert target.sv.r.z == 0.0
        # Velocities converted from km/s to m/s
        assert target.sv.v.x == 0.0
        assert target.sv.v.y == 7500.0
        assert target.sv.v.z == 0.0

    def test_state_vector_j2000_frame(self, program):
        request = MockCollectRequest.with_state_vector()
        target = program._build_target(request)

        assert target.frame == ReferenceFrame.GCRF

    def test_state_vector_teme_frame(self, program):
        request = MockCollectRequest.with_state_vector()
        request.state_vector.reference_frame = "TEME"
        target = program._build_target(request)

        assert target.frame == ReferenceFrame.TEME


class TestBuildTargetRADec:
    def test_radec_target(self, program):
        request = MockCollectRequest.with_radec(ra=180.0, dec=45.0)
        target = program._build_target(request)

        assert isinstance(target, ICRSTarget)
        assert target.coords.ra == 180.0
        assert target.coords.dec == 45.0


class TestBuildTargetPriority:
    def test_tle_preferred_over_radec(self, program):
        """When both TLE and RA/Dec are present, TLE takes priority."""
        request = MockCollectRequest.with_tle()
        request.ra = 180.0
        request.dec = 45.0
        target = program._build_target(request)

        assert isinstance(target, TLETarget)

    def test_no_target_data(self, program):
        """Returns None when no target data is available."""
        request = MockCollectRequest.create()
        target = program._build_target(request)

        assert target is None
