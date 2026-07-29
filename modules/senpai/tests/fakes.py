# SPDX-License-Identifier: Apache-2.0
"""Stand-ins for the SENPAI engine.

`FakePipeline` replaces `SenpaiPipeline`, which wraps `astro-senpai`'s collect pipeline: it
records each run and hands back one result per input frame, so the analyzer's batching can be
exercised without the engine or real FITS data. `make_result` builds the results it returns.
"""

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sensorkit.senpai.models import SenpaiResult
from sensorkit.senpai.pipeline import FrameInput


def make_result(file_path="/data/raw/frame.fits", **overrides) -> SenpaiResult:
    fields = dict(
        file_path=file_path,
        timestamp=datetime(2026, 7, 9, 1, 2, 3, tzinfo=UTC),
        track_mode="SIDEREAL",
        n_sources=0,
        median_fwhm_pixels=None,
        std_fwhm_pixels=None,
        pixel_scale_arcsec=None,
        median_fwhm_arcsec=None,
        std_fwhm_arcsec=None,
        solved=False,
        detections=[],
    )
    fields.update(overrides)
    return SenpaiResult(**fields)


@dataclass
class FakePipeline:
    """Stands in for SenpaiPipeline, recording the inputs of every run.

    `delay` gives a run measurable wall-clock duration, as a real collect has. The analyzer runs
    the pipeline in a worker thread, so blocking here leaves the event loop free — which is what
    lets a stalled sequence be told apart from one merely waiting on a slow run.
    """

    delay: float = 0.0
    runs: list[tuple[list[FrameInput], bool]] = field(default_factory=list)

    def process_frames(
        self, inputs: list[FrameInput], from_sequence: bool = False
    ) -> list[SenpaiResult]:
        if self.delay:
            time.sleep(self.delay)

        self.runs.append((list(inputs), from_sequence))

        return [
            make_result(file_path=inp.file_path, from_sequence=from_sequence) for inp in inputs
        ]

    def batch_sizes(self) -> list[int]:
        """How many frames each run received, in run order."""
        return [len(inputs) for inputs, _ in self.runs]
