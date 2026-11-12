# SPDX-License-Identifier: Apache-2.0
import json
from datetime import UTC, datetime

from pydantic import TypeAdapter

from sensorkit.common.keyword import KeywordDict
from sensorkit.data.context import Context
from sensorkit.std.instrument import ExposureInfo


def test_exposure_info_uses_the_exposure_context_key():
    info = ExposureInfo(
        date_obs=datetime(2026, 11, 12, 3, 4, 5, tzinfo=UTC),
        exposure_time=2.5,
        instrument="mycamera",
    )
    adapter = TypeAdapter(KeywordDict)
    wire = adapter.dump_json(KeywordDict(info))

    assert json.loads(wire) == {"Exposure": info.model_dump(mode="json")}

    restored = adapter.validate_json(wire)
    assert set(restored) == {"Exposure"}
    assert isinstance(restored["Exposure"], ExposureInfo)
    assert restored[ExposureInfo] == info

    context = Context(restored)
    assert context.eval("Exposure.date_obs") == info.date_obs
    assert context.resolve("{Exposure.date_obs:%Y%m%d}.fits") == "20261112.fits"
