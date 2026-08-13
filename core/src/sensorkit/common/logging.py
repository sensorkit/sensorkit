# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import functools
import os
import pathlib
import sys
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Self, cast

from loguru import logger

if TYPE_CHECKING:
    from loguru import Logger
else:
    Logger = Any

DEFAULT_DEBUG_FILE = "sensorkit.log"
DEFAULT_LIMIT_INTERVAL = 5.0


def _format_time(dt: datetime):
    return dt.strftime(f"[%m/%d/%y %H:%M:%S.{dt.microsecond // 1000}]")


def configure_logging(
    *,
    level: str | None = None,
    format: str | Callable | None = None,
    force_color: bool = True,
):
    if level is None:
        level = "DEBUG" if os.environ.get("SENSORKIT_DEBUG") else "INFO"

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


class NullLogger:
    """Stand-in for a loguru logger that discards everything sent to it.

    Any attribute resolves to a call accepting anything and returning the same
    object, so chained forms such as `opt(exception=e).warning(...)` stay valid.
    """

    def __getattr__(self, name: str) -> Callable[..., Self]:
        return self.discard

    def discard(self, *args: Any, **kwargs: Any) -> Self:
        return self


class RateLimiter:
    __slots__ = ("deadline",)

    def __init__(self):
        self.deadline = float("-inf")

    def allow(self, interval: float) -> bool:
        now = time.monotonic()

        if now < self.deadline:
            return False

        self.deadline = now + interval
        return True


NULL_LOGGER = cast(Logger, cast(object, NullLogger()))


@functools.lru_cache(maxsize=1024)
def _logger_limiter(_: str) -> RateLimiter:
    return RateLimiter()


def limited_logger(subject: str | None = None, *, interval: float = DEFAULT_LIMIT_INTERVAL) -> Logger:
    """Get a logger that emits at most once per interval for the calling site.

    The limit applies to the call site rather than to message content, so while a
    site is limited, every call made through the returned logger is discarded. The
    first call always emits and opens the next window.

    The result *must* used immediately, as in `limited_logger().info(...)`.
    Acquiring it is what consumes the window, so holding one in a variable and
    reusing it defeats the limit. For the same reason, wrapping this function in a
    helper keys every one of that helper's callers to the helper's own line.

    Args:
        subject: Optional qualifier dividing one call site into independent limits.
            Sites are keyed by file and line, so equal subjects arising at different
            call sites never share a window.
        interval: Minimum seconds between emissions. It is read on every call, so a
            site can be limited at different rates as conditions change.

    Returns:
        The loguru logger when the call site is outside its window, otherwise a
        stand-in that discards everything sent to it.
    """
    import inspect

    frame = inspect.currentframe()
    caller = frame.f_back if frame is not None else None

    if caller is None:
        # No frame support, so there is no call site to key on. Emit unlimited
        # rather than share one window across every unrelated call site.
        return logger

    key = f"{caller.f_code.co_filename}:{caller.f_lineno}"

    if subject is not None:
        key = f"{key}:{subject}"

    if _logger_limiter(key).allow(interval):
        return logger

    return NULL_LOGGER
