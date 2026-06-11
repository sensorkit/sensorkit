"""TEMPORARY shim for the focus calibration task.

This mirrors the old ``sensorkit.std.calibrate`` module verbatim. It exists only so the
autofocus module imports and unit-tests standalone on ``dev``, which does not yet carry
calibration-task support. The autofocus *program* constructs and queues a
``FocusCalibrateTask``; the actual focus-sweep execution belongs to the controller side
(a ``sensor_calibrate`` task handler on ``SensorControl``), which is being folded into
core separately.

REMOVE this file once core lands the real ``FocusCalibrateTask`` (expected at
``sensorkit.std.calibrate``) and re-point the imports in ``program.py`` / ``task_queue.py``.
"""

from datetime import datetime
from typing import Literal

from sensorkit.astro.target import Target
from sensorkit.core.task import CalibrateTask


class FocusCalibrateTask(CalibrateTask):
    """A calibration task that performs a focus sweep."""
    task_type: Literal["focus"] = "focus"
    target: Target | None = None
    start_position: float
    end_position: float
    num_steps: int
    filter_name: str | None = None
    exposure_time: float
    binning: int = 1
    end_time: datetime
