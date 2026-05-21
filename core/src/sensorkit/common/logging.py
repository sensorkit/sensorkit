import os
import pathlib
import sys
from datetime import datetime
from typing import Callable

from loguru import logger

DEFAULT_DEBUG_FILE = "sensorkit.log"


def _format_time(dt: datetime):
    return dt.strftime(f"[%m/%d/%y %H:%M:%S.{dt.microsecond // 1000}]")


def configure_logging(
    *,
    level="INFO",
    format: str | Callable | None = None,
    force_color: bool = True,
):
    if force_color:
        os.environ["FORCE_COLOR"] = "1"

    if format is None:
        format = (
            "<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> "
            "<level>{level: <8}</level> "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>  "
            "<level>{message}</level>"
        )

    logger.remove()
    logger.add(
        sys.stderr,
        format=format,
        level=level,
        colorize=True,
        diagnose=False,
        backtrace=False,
    )


def add_debug_logger(
    *,
    file: str | None = None,
    syslog_id: str = "sensorkit",
    append: bool = True,
    backtrace: bool = True,
    diagnose: bool = True,
) -> str:
    if file is None:
        try:
            from systemd.journal import JournalHandler

            logger.add(
                JournalHandler(SYSLOG_IDENTIFIER=syslog_id),
                level="DEBUG",
                colorize=False,
                enqueue=True,
                backtrace=backtrace,
                diagnose=diagnose,
            )
            return f"system log -- run: `journalctl -t {syslog_id} -f` to watch"
        except ImportError:
            # systemd not available, fall back to default.
            pass

    path = pathlib.Path(file or DEFAULT_DEBUG_FILE)
    logger.add(
        path,
        mode="a" if append else "w",
        level="DEBUG",
        colorize=False,
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )
    return str(path.absolute())
