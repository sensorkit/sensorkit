"""Logging configuration helpers using loguru."""

import os
import sys
from datetime import datetime

from loguru import logger


def _format_time(dt: datetime):
    return dt.strftime(f"[%m/%d/%y %H:%M:%S.{dt.microsecond // 1000}]")


def configure_logging(level="DEBUG", log_file=None, log_file_append=True):
    """Set up loguru sinks for stderr and, optionally, a log file.

    Stderr always receives DEBUG-level colorized output. When ``log_file`` is provided a second
    sink is added at ``level`` with full backtrace and diagnostic information enabled.
    """
    os.environ["FORCE_COLOR"] = "1"

    logger.remove()
    logger.add(
        sys.stderr,
        format=(
            "<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> "
            "<level>{level: <8}</level> "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>  "
            "<level>{message}</level>"
        ),
        level="DEBUG",
        colorize=True,
        diagnose=False,
        backtrace=False,
    )

    if log_file:
        logger.add(
            log_file,
            mode="a" if log_file_append else "w",
            level=level,
            colorize=False,
            enqueue=True,
            backtrace=True,
            diagnose=True,
        )
