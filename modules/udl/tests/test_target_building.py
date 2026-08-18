# SPDX-License-Identifier: Apache-2.0
import pytest

from sensorkit.astro.common import ReferenceFrame
from sensorkit.astro.target import ICRSTarget, StateVectorTarget, TLETarget
from sensorkit.udl.models import UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram

from .fakes import (
    make_collect_request,
    radec_request,
    state_vector_request,
    tle_request,
)


@pytest.fixture
def program():
    config = UDLConfig(
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
        request = tle_request(sat_no=25544)
        target = program._build_target(request)

        assert isinstance(target, TLETarget)
        assert target.tle.line0 == "0 25544"
        assert "25544" in target.tle.line1

    def test_tle_target_no_sat_no(self, program):
        request = tle_request(sat_no=None, orig_object_id="99999")
        target = program._build_target(request)

        assert isinstance(target, TLETarget)
        assert target.tle.line0 == "0 99999"


class TestBuildTargetStateVector:
    def test_state_vector_target(self, program):
        request = state_vector_request()
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
        request = state_vector_request()
        target = program._build_target(request)

        assert target.frame == ReferenceFrame.GCRF

    def test_state_vector_teme_frame(self, program):
        request = state_vector_request()
        request.state_vector.reference_frame = "TEME"
        target = program._build_target(request)

        assert target.frame == ReferenceFrame.TEME


class TestBuildTargetRADec:
    def test_radec_target(self, program):
        request = radec_request(ra=180.0, dec=45.0)
        target = program._build_target(request)

        assert isinstance(target, ICRSTarget)
        assert target.coords.ra == 180.0
        assert target.coords.dec == 45.0


class TestBuildTargetPriority:
    def test_tle_preferred_over_radec(self, program):
        """When both TLE and RA/Dec are present, TLE takes priority."""
        request = tle_request()
        request.ra = 180.0
        request.dec = 45.0
        target = program._build_target(request)

        assert isinstance(target, TLETarget)

    def test_no_target_data(self, program):
        """Returns None when no target data is available."""
        request = make_collect_request()
        target = program._build_target(request)

        assert target is None


class TestTrackMode:
    """`type` -> sidereal frame indices (0-based). Free-string match, so the
    same rules serve UDL and UDL-compliant endpoints alike."""

    def test_rate_track_is_all_rate(self, program):
        assert program._track_mode(tle_request(type="RATE TRACK", num_frames=3)) == []

    def test_object_is_all_rate(self, program):
        assert program._track_mode(tle_request(type="OBJECT", num_frames=4)) == []

    def test_compound_type_is_last_frame_only(self, program):
        # "RATE TRACK SIDEREAL": rate-track with only the final frame sidereal.
        assert program._track_mode(tle_request(type="RATE TRACK SIDEREAL", num_frames=3)) == [2]

    def test_stare_is_all_sidereal(self, program):
        assert program._track_mode(tle_request(type="STARE", num_frames=3)) == [0, 1, 2]

    def test_sidereal_is_all_sidereal(self, program):
        assert program._track_mode(tle_request(type="SIDEREAL", num_frames=2)) == [0, 1]

    def test_match_is_case_insensitive(self, program):
        assert program._track_mode(tle_request(type="rate track sidereal", num_frames=5)) == [4]

    def test_unrecognized_type_defaults_to_rate(self, program):
        assert program._track_mode(tle_request(type="DWELL", num_frames=3)) == []
