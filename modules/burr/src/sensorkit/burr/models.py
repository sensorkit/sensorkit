# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel


class CollectConfig(BaseModel):
    filter_name: str | None = None
    readout_mode: int | None = None
    gain: float | None = None
    binning: int | None = None
