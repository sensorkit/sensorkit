# SPDX-License-Identifier: Apache-2.0
from pydantic import BaseModel, Field


class CollectConfig(BaseModel):
    """Camera settings shared by a Burr program's collect tasks."""

    instrument: str | None = Field(default=None, min_length=1)
    """Controller instrument name, or None for automatic selection."""
    filter_name: str | None = None
    readout_mode: int | None = None
    gain: float | None = None
    binning: int | None = None
