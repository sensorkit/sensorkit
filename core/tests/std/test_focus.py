# SPDX-License-Identifier: Apache-2.0
"""Tests for focuser commands and focus analysis schemas."""

import json
import math
from datetime import UTC, datetime
from uuid import UUID

import pytest
from astropy.io import fits
from pydantic import ValidationError

import sensorkit.api as sk
import sensorkit.std
from sensorkit.common.keyword import (
    dump_keyword_json,
    get_keyword_info,
    validate_keyword,
    validate_keyword_json,
)
from sensorkit.core.entity import DeviceDetails, EntityDetails
from sensorkit.core.trait import match_archetype, trait_keyword_ids
from sensorkit.std import focus, optics
from sensorkit.std.focus import (
    AutoFocus,
    ChangeFocusPosition,
    FocusAnalysis,
    FocusAnalysisSource,
    FocusPosition,
    FocusProvider,
    FocusSweepAnalysis,
    FocusSweepContext,
    StandardFocuser,
)
from sensorkit.std.instrument import Binning, ExposureInfo, FrameType

SWEEP_ID = UUID("6f1c2b7e-3d4a-4c1b-9e2f-0a1b2c3d4e5f")
OTHER_SWEEP_ID = UUID("0b8e6a52-91f4-4b7d-8c3e-5d6f7a8b9c0d")


def exposure(**overrides) -> ExposureInfo:
    fields = {
        "date_obs": datetime(2026, 10, 14, 8, 30, tzinfo=UTC),
        "exposure_time": 2.0,
        "detector": "QHY600M",
        "image_type": FrameType.LIGHT,
        "readout_mode": 1,
        "gain": 26.0,
    }
    return ExposureInfo(**(fields | overrides))


def sweep_context(index: int = 0, **overrides) -> FocusSweepContext:
    fields = {
        "sweep_id": SWEEP_ID,
        "sample_index": index,
        "sample_count": 3,
        "commanded_position": 1000.0 + 50.0 * index,
    }
    return FocusSweepContext(**(fields | overrides))


def analysis(**overrides) -> FocusAnalysis:
    fields = {
        "controller_id": "controller",
        "instrument": "camera",
        "image_id": "/data/focus/frame-0.fits",
        "exposure": exposure(),
        "reported_position": 1001.0,
        "sweep": sweep_context(),
        "median_fwhm_pixels": 3.2,
        "source_count": 42,
        "valid": True,
    }
    return FocusAnalysis(**(fields | overrides))


def invalid_analysis(**overrides) -> FocusAnalysis:
    fields = {"valid": False, "median_fwhm_pixels": None, "source_count": 0, "reason": "no stars"}
    return analysis(**(fields | overrides))


def sweep_analysis(**overrides) -> FocusSweepAnalysis:
    fields = {
        "controller_id": "controller",
        "instrument": "camera",
        "sweep_id": SWEEP_ID,
        "outcome": "success",
        "position": 1040.0,
        "disposition": "equals",
        "rms_residual_pixels": 0.12,
        "samples": [analysis(sweep=sweep_context(i)) for i in range(3)],
    }
    return FocusSweepAnalysis(**(fields | overrides))


def failed_sweep_analysis(**overrides) -> FocusSweepAnalysis:
    fields = {
        "outcome": "failure",
        "position": None,
        "disposition": None,
        "rms_residual_pixels": None,
        "reason": "fit did not converge",
    }
    return sweep_analysis(**(fields | overrides))


def header_from(*providers) -> fits.Header:
    header = fits.Header()

    for provider in providers:
        for keyword, card in provider.get_fits_cards():
            header[keyword] = card

    # Pass through the serialized header text, as a reader of the image file would.
    return fits.Header.fromstring(header.tostring())


class TestAutoFocus:
    def test_round_trip_keeps_order_and_settings(self):
        task = AutoFocus(
            instrument="camera",
            positions=[1000.0, 1050.0, 1050.0, 975.0],
            integration_time_seconds=2.0,
            binning=Binning(x=2, y=2),
            gain=26.0,
            readout_mode=1,
            filter_name="L",
        )

        restored = sk.Task.model_validate_json(task.model_dump_json())

        assert AutoFocus.model_tag() == "autofocus"
        assert type(restored) is AutoFocus
        assert restored == task
        assert restored.positions == [1000.0, 1050.0, 1050.0, 975.0]

    def test_acquisition_settings_are_optional(self):
        task = AutoFocus(instrument="camera", positions=[1000.0], integration_time_seconds=1.0)

        assert (task.binning, task.gain, task.readout_mode, task.filter_name) == (None,) * 4

    @pytest.mark.parametrize(
        "overrides",
        [
            {"positions": []},
            {"positions": [1000.0, math.nan]},
            {"positions": [math.inf]},
            {"integration_time_seconds": 0},
            {"integration_time_seconds": -1.0},
            {"integration_time_seconds": math.nan},
            {"integration_time_seconds": math.inf},
            {"instrument": ""},
        ],
    )
    def test_rejects_invalid_sweep(self, overrides):
        fields = {"instrument": "camera", "positions": [1000.0], "integration_time_seconds": 1.0}

        with pytest.raises(ValidationError):
            AutoFocus(**(fields | overrides))


class TestFocusAnalysisSource:
    @pytest.mark.parametrize(
        "entity", ["analyzer", sk.EntityRef("analyzer"), sk.DeviceRef("analyzer")]
    )
    def test_config_round_trip(self, entity):
        source = validate_keyword("FocusAnalysisSource", {"entity": entity})
        restored = validate_keyword_json("FocusAnalysisSource", dump_keyword_json(source))

        assert get_keyword_info(FocusAnalysisSource).kind == "config"
        assert json.loads(dump_keyword_json(source)) == {"entity": "analyzer"}
        assert type(restored) is FocusAnalysisSource
        assert restored.entity == sk.EntityRef("analyzer")

    @pytest.mark.parametrize(
        "entity, message",
        [
            (sk.EntityRef(), "nonempty entity name"),
            ("", "nonempty entity name"),
            (None, "nonempty entity name"),
            (sk.EntityRef(5), "nonempty entity name"),
            (5, "Expected an entity name or EntityRef"),
        ],
    )
    def test_rejects_invalid_entity(self, entity, message):
        with pytest.raises(ValidationError, match=message):
            FocusAnalysisSource(entity=entity)


class TestFocusSweepContext:
    def test_keyword_round_trip_keeps_sweep_id(self):
        context = sweep_context(2)
        restored = validate_keyword_json("FocusSweepContext", dump_keyword_json(context))

        assert restored == context
        assert restored.sweep_id == SWEEP_ID

    @pytest.mark.parametrize(
        "overrides",
        [
            {"sample_index": 3},
            {"sample_index": 4},
            {"sample_index": -1},
            {"sample_count": 0},
            {"commanded_position": math.nan},
        ],
    )
    def test_rejects_invalid_membership(self, overrides):
        with pytest.raises(ValidationError):
            sweep_context(**overrides)

    def test_fits_round_trip(self):
        context = sweep_context(1, commanded_position=1234.5)

        assert FocusSweepContext.from_fits_header(header_from(context)) == context

    def test_fits_cards_exclude_controller_and_instrument(self):
        cards = dict(sweep_context().get_fits_cards())

        assert set(cards) == {"FOCSWEEP", "FOCSAMP", "FOCCOUNT", "FOCCMD"}

    def test_ordinary_header_has_no_context(self):
        header = fits.Header({"INSTRUME": "QHY600M", "SKCTRL": "controller"})

        assert FocusSweepContext.from_fits_header(header) is None

    def test_partial_header_raises_key_error(self):
        header = header_from(sweep_context())
        del header["FOCCMD"]

        with pytest.raises(KeyError):
            FocusSweepContext.from_fits_header(header)

    @pytest.mark.parametrize(
        "keyword, value",
        [
            ("FOCSWEEP", "not-a-uuid"),
            ("FOCSAMP", "first"),
            ("FOCSAMP", 3),
            ("FOCCMD", "far"),
        ],
    )
    def test_invalid_header_value_raises_validation_error(self, keyword, value):
        header = header_from(sweep_context())
        header[keyword] = value

        with pytest.raises(ValidationError):
            FocusSweepContext.from_fits_header(header)


class TestCaptureMetadataCards:
    def test_focus_position_round_trip(self):
        position = FocusPosition(position=1012.5)

        assert FocusPosition.from_fits_header(header_from(position)) == position

    def test_header_without_focus_position(self):
        assert FocusPosition.from_fits_header(header_from(sweep_context())) is None

    def test_exposure_round_trip(self):
        recorded = exposure(
            offset=10.0, ccd_temperature=-10.0, set_temperature=-10.0, max_adu=65535
        )

        assert ExposureInfo.from_fits_header(header_from(recorded)) == recorded

    def test_exposure_round_trip_keeps_only_written_cards(self):
        recorded = ExposureInfo(
            date_obs=datetime(2026, 10, 14, 8, 30, 1, 250000, tzinfo=UTC),
            exposure_time=0.5,
            readout_mode="High",
        )

        assert ExposureInfo.from_fits_header(header_from(recorded)) == recorded

    @pytest.mark.parametrize("card", ["DATE-OBS", "EXPTIME"])
    def test_header_without_an_exposure_record(self, card):
        header = header_from(exposure())
        del header[card]

        assert ExposureInfo.from_fits_header(header) is None

    def test_exposure_time_zone_is_kept(self):
        header = header_from(exposure())
        header["DATE-OBS"] = "2026-10-14T18:30:00+10:00"

        assert ExposureInfo.from_fits_header(header).date_obs == exposure().date_obs

    @pytest.mark.parametrize(("card", "value"), [("DATE-OBS", "evening"), ("EXPTIME", "long")])
    def test_invalid_exposure_card_raises_value_error(self, card, value):
        header = header_from(exposure())
        header[card] = value

        with pytest.raises(ValueError):
            ExposureInfo.from_fits_header(header)

    def test_instrument_card_names_the_controller_instrument(self):
        header = header_from(exposure(), sk.InstrumentInfo(name="cam-east", devices=("foc",)))

        assert (header["INSTRUME"], header["DETECTOR"]) == ("cam-east", "QHY600M")


class TestFocusAnalysis:
    def test_keyword_round_trip_keeps_exposure(self):
        result = analysis()
        restored = validate_keyword_json("FocusAnalysis", dump_keyword_json(result))

        assert type(restored) is FocusAnalysis
        assert restored == result
        assert type(restored.exposure) is ExposureInfo
        assert (restored.exposure.gain, restored.exposure.readout_mode) == (26.0, 1)
        assert type(restored.sweep) is FocusSweepContext

    def test_image_outside_sweep(self):
        result = analysis(sweep=None, reported_position=None)
        restored = validate_keyword_json("FocusAnalysis", dump_keyword_json(result))

        assert restored.sweep is None
        assert restored.reported_position is None

    def test_requires_exposure(self):
        data = analysis().model_dump(exclude={"exposure"})

        with pytest.raises(ValidationError, match="exposure"):
            FocusAnalysis.model_validate(data)

    def test_invalid_measurement_keeps_evidence(self):
        result = invalid_analysis(median_fwhm_pixels=9.5, source_count=2, reason="too few stars")

        assert (result.median_fwhm_pixels, result.source_count) == (9.5, 2)

    def test_controller_round_trips_as_name(self):
        result = analysis()
        restored = FocusAnalysis.model_validate_json(result.model_dump_json())

        assert json.loads(result.model_dump_json())["controller_id"] == "controller"
        assert type(restored.controller_id) is sk.ControllerRef
        assert restored.controller_id == sk.ControllerRef("controller")

    @pytest.mark.parametrize(
        "controller_id, message",
        [
            (sk.ControllerRef(), "nonempty entity name"),
            ("", "nonempty entity name"),
            (None, "nonempty entity name"),
            (sk.ControllerRef(5), "nonempty entity name"),
            (5, "Expected an entity name or ControllerRef"),
            (sk.EntityRef("controller"), "Expected an entity name or ControllerRef"),
        ],
    )
    def test_rejects_invalid_controller(self, controller_id, message):
        with pytest.raises(ValidationError, match=message):
            analysis(controller_id=controller_id)

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"reason": "blurred"}, "no failure reason"),
            ({"median_fwhm_pixels": None}, "requires a FWHM"),
            ({"source_count": 0}, "requires a source"),
            ({"valid": False}, "requires a reason"),
            ({"median_fwhm_pixels": 0.0}, "greater than 0"),
            ({"median_fwhm_pixels": math.inf}, "finite"),
            ({"source_count": -1}, "greater than or equal to 0"),
            ({"reason": ""}, "at least 1 character"),
        ],
    )
    def test_rejects_inconsistent_measurement(self, overrides, match):
        with pytest.raises(ValidationError, match=match):
            analysis(**overrides)


class TestFocusSweepAnalysis:
    def test_keyword_round_trip_keeps_nested_evidence(self):
        result = sweep_analysis()
        restored = validate_keyword_json("FocusSweepAnalysis", dump_keyword_json(result))

        assert type(restored) is FocusSweepAnalysis
        assert restored == result
        assert [type(sample) for sample in restored.samples] == [FocusAnalysis] * 3
        assert [sample.sweep.sample_index for sample in restored.samples] == [0, 1, 2]
        assert type(restored.controller_id) is sk.ControllerRef

    @pytest.mark.parametrize("disposition", ["greater_than", "less_than"])
    def test_directional_success_round_trips(self, disposition):
        result = sweep_analysis(position=1100.0, disposition=disposition)
        restored = validate_keyword_json("FocusSweepAnalysis", dump_keyword_json(result))

        assert (restored.position, restored.disposition) == (1100.0, disposition)

    def test_success_needs_only_one_valid_sample(self):
        samples = [analysis(sweep=sweep_context(0)), invalid_analysis(sweep=sweep_context(1))]

        assert sweep_analysis(samples=samples).samples == samples

    @pytest.mark.parametrize(
        "samples",
        [
            [],
            [invalid_analysis(sweep=sweep_context(0))],
        ],
    )
    def test_failure_keeps_partial_evidence(self, samples):
        result = failed_sweep_analysis(samples=samples, rms_residual_pixels=4.5)
        restored = validate_keyword_json("FocusSweepAnalysis", dump_keyword_json(result))

        assert restored == result
        assert restored.rms_residual_pixels == 4.5

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"reason": "noisy"}, "no failure reason"),
            ({"position": None}, "requires a position"),
            ({"disposition": None}, "requires a disposition"),
            ({"disposition": "near"}, "Input should be"),
            ({"rms_residual_pixels": None}, "requires a residual"),
            ({"rms_residual_pixels": -0.1}, "greater than or equal to 0"),
            ({"samples": []}, "requires a valid sample"),
            ({"samples": [invalid_analysis()]}, "requires a valid sample"),
            ({"position": math.nan}, "finite"),
        ],
    )
    def test_rejects_inconsistent_success(self, overrides, match):
        with pytest.raises(ValidationError, match=match):
            sweep_analysis(**overrides)

    @pytest.mark.parametrize(
        "overrides, match",
        [
            ({"reason": None}, "requires a reason"),
            ({"position": 1040.0}, "has no position"),
            ({"disposition": "less_than"}, "has no disposition"),
        ],
    )
    def test_rejects_inconsistent_failure(self, overrides, match):
        with pytest.raises(ValidationError, match=match):
            failed_sweep_analysis(**overrides)

    @pytest.mark.parametrize(
        "sample, match",
        [
            (analysis(sweep=None), "context from this sweep"),
            (analysis(sweep=sweep_context(sweep_id=OTHER_SWEEP_ID)), "context from this sweep"),
            (analysis(controller_id="other"), "controller must match"),
            (analysis(instrument="other"), "instrument must match"),
        ],
    )
    def test_rejects_unrelated_evidence(self, sample, match):
        with pytest.raises(ValidationError, match=match):
            failed_sweep_analysis(samples=[sample])

    @pytest.mark.parametrize(
        "controller_id, message",
        [
            (sk.ControllerRef(), "nonempty entity name"),
            ("", "nonempty entity name"),
            (None, "nonempty entity name"),
            (sk.ControllerRef(5), "nonempty entity name"),
            (5, "Expected an entity name or ControllerRef"),
            (sk.EntityRef("controller"), "Expected an entity name or ControllerRef"),
        ],
    )
    def test_rejects_invalid_controller(self, controller_id, message):
        with pytest.raises(ValidationError, match=message):
            failed_sweep_analysis(controller_id=controller_id, samples=[])


class TestTraits:
    @pytest.mark.parametrize(
        "details, expected",
        [
            (EntityDetails(published_keywords={"FocusAnalysis", "FocusSweepAnalysis"}), True),
            (EntityDetails(published_keywords={"FocusAnalysis"}), False),
            (EntityDetails(published_keywords={"FocusSweepAnalysis"}), False),
            (EntityDetails(), False),
            (None, False),
        ],
    )
    def test_focus_provider_requires_both_publications(self, details, expected):
        assert FocusProvider.match(details) is expected

    def test_focuser_subscribes_focus_position(self):
        details = DeviceDetails(supported_commands={"ChangeFocusPosition"})

        assert StandardFocuser.effective_keyword_ids() == {"FocusPosition"}
        assert match_archetype(details) == StandardFocuser
        assert "FocusPosition" in trait_keyword_ids(details)

    def test_focuser_identifiers_are_unchanged(self):
        assert StandardFocuser.name == "focuser"
        assert ChangeFocusPosition.model_tag() == "ChangeFocusPosition"
        assert get_keyword_info(FocusPosition).key == "FocusPosition"

    def test_root_exports_are_relocated_objects(self):
        exported = [
            (sensorkit.std.AutoFocus, focus.AutoFocus),
            (sensorkit.std.ChangeFocusPosition, focus.ChangeFocusPosition),
            (sensorkit.std.FocusAnalysis, focus.FocusAnalysis),
            (sensorkit.std.FocusAnalysisSource, focus.FocusAnalysisSource),
            (sensorkit.std.FocusPosition, focus.FocusPosition),
            (sensorkit.std.FocusProvider, focus.FocusProvider),
            (sensorkit.std.FocusSweepAnalysis, focus.FocusSweepAnalysis),
            (sensorkit.std.FocusSweepContext, focus.FocusSweepContext),
            (sensorkit.std.StandardFocuser, focus.StandardFocuser),
        ]

        assert all(root is leaf for root, leaf in exported)
        assert {"ChangeFocusPosition", "FocusPosition", "StandardFocuser"}.isdisjoint(vars(optics))
