# SPDX-License-Identifier: Apache-2.0
import pytest
from pydantic import ValidationError

from sensorkit.senpai.models import Detection, SenpaiConfig

from .fakes import make_result


class TestDetection:
    def test_kind_defaults_to_none(self):
        det = Detection(x=1.0, y=2.0)
        assert det.kind is None

    def test_kind_accepts_senpai_discriminators(self):
        for kind in ("streak", "point", "streak_candidate"):
            assert Detection(x=1.0, y=2.0, kind=kind).kind == kind

    def test_kind_rejects_unknown_values(self):
        with pytest.raises(ValidationError):
            Detection(x=1.0, y=2.0, kind="noise")


class TestSenpaiResult:
    def test_pass_through_fields_default_to_absent(self):
        result = make_result()
        assert result.task_id is None
        assert result.frame_num is None
        assert result.frame_count is None
        assert result.from_sequence is True
        assert result.exposure_time_seconds is None


class TestSenpaiConfig:
    def test_sequence_processing_on_by_default(self):
        config = SenpaiConfig(senpai_config="/x.yaml", senpai_output_dir="/out")
        assert config.process_sequence is True
