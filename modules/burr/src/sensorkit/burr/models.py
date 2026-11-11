# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Annotated, Self

from pydantic import BaseModel, Field, SerializationInfo, field_serializer, model_validator


class CameraSettings(BaseModel):
    """Camera settings for capture requests. `None` leaves a setting unspecified."""

    filter_name: str | None = None
    readout_mode: int | None = None
    gain: float | None = None
    binning: int | None = None

    def with_defaults(self, shared: CameraSettings) -> CameraSettings:
        """Fill omitted fields from shared settings, preserving explicit `None`."""
        return shared.model_copy(update=self.model_dump(exclude_unset=True))


class CollectConfig(CameraSettings):
    """Shared camera settings and instrument selections for a Burr program.

    Attributes:
        instrument: Controller instrument name, or `None` for automatic selection.
        instruments: Instrument names and camera overrides in configured order.
            Omitted fields inherit shared settings. Explicit `None` clears an
            inherited value and leaves the setting unspecified.
    """

    instrument: str | None = Field(default=None, min_length=1)
    instruments: dict[Annotated[str, Field(min_length=1)], CameraSettings] | None = Field(
        default=None, min_length=1
    )

    @model_validator(mode="after")
    def _check_selection(self) -> Self:
        if self.instrument is not None and self.instruments is not None:
            raise ValueError("set either instrument or instruments, not both")

        return self

    @field_serializer("instruments")
    def _dump_overrides(
        self, instruments: dict[str, CameraSettings] | None, info: SerializationInfo
    ) -> dict[str, dict] | None:
        # Keep omitted overrides distinct from explicit null when saving.
        if instruments is None:
            return None

        return {
            name: override.model_dump(mode=info.mode, exclude_unset=True)
            for name, override in instruments.items()
        }

    def instrument_settings(self) -> list[tuple[str | None, CameraSettings]]:
        """Resolve instrument names and camera settings in configured order."""
        shared = CameraSettings.model_validate(
            self.model_dump(include=set(CameraSettings.model_fields))
        )

        if self.instruments is None:
            return [(self.instrument, shared)]

        return [
            (name, override.with_defaults(shared)) for name, override in self.instruments.items()
        ]
