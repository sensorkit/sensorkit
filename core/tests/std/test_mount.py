# SPDX-License-Identifier: Apache-2.0
from sensorkit.std import AxisRate, MountAxis, RateSource


def test_velocity_source_defaults_to_none():
    rate = AxisRate.model_validate({"axis": "right_ascension", "velocity": 0.01})
    assert rate.velocity_source is None


def test_velocity_source_round_trips():
    rate = AxisRate(
        axis=MountAxis.RIGHT_ASCENSION, velocity=0.01, velocity_source=RateSource.COMMANDED
    )
    restored = AxisRate.model_validate_json(rate.model_dump_json())
    assert restored.velocity_source is RateSource.COMMANDED
