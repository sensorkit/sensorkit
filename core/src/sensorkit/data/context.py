from __future__ import annotations

import asyncio
import builtins
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, overload

from pydantic import BaseModel

from sensorkit.common.keyword import KeywordDict

if TYPE_CHECKING:
    from sensorkit.core.entity import EntityClient

_SAFE_BUILTINS = {
    "__import__": builtins.__import__,
    "format": format,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "round": round,
    "len": len,
    "min": min,
    "max": max,
    "abs": abs,
}

_EVAL_GLOBALS = {"__builtins__": _SAFE_BUILTINS, "datetime": datetime, "UTC": UTC}
_MISSING = object()
_FSTRING_OPENERS = ('f"', "f'", 'F"', "F'")


def _raw_fstring_source(value: str) -> str:
    """Build source that evaluates `value` as a raw f-string template.

    The value is embedded in a triple-double-quoted raw f-string, so backslashes stay
    literal while `{...}` still interpolates. Trailing backslashes or quotes cannot safely
    abut the closing delimiter (a raw literal cannot end in an odd backslash run, and a
    trailing quote would merge with it), so they are peeled off and re-appended verbatim
    via concatenation. A triple-quote embedded mid-value is unsupported and surfaces as a
    `SyntaxError` when the source is evaluated.

    Args:
        value: Template text containing one or more `{...}` fields.

    Returns:
        Python source that evaluates to the rendered string.
    """
    end = len(value)

    while end and value[end - 1] in '\\"':
        end -= 1

    source = f'rf"""{value[:end]}"""'

    if end < len(value):
        source += f" + {value[end:]!r}"

    return source


class Context(KeywordDict):
    """A KeywordDict for storing data context."""

    def eval(self, expr: str, *, default: object = _MISSING):
        """Evaluate a Python expression with this Context as the namespace.

        Names in the expression resolve against this Context's keys.

        Args:
            expr: The Python expression to evaluate.
            default: Value returned if `expr` references a name absent from the Context.
                If omitted, a missing name raises `NameError`. Only missing names are
                caught; any other error (such as a bad format spec on a present value)
                always propagates.

        Returns:
            The expression result, keeping its native type.

        Raises:
            NameError: A referenced name is absent and no `default` was given.
        """
        try:
            return eval(expr, _EVAL_GLOBALS, self)
        except NameError:
            if default is _MISSING:
                raise

            return default

    @overload
    def resolve[T](self, value: str | None, *, as_type: type[T], default: T = ...) -> T: ...

    @overload
    def resolve(self, value: str | None, *, default: object = ...) -> object: ...

    def resolve(self, value: str | None, *, as_type = _MISSING, default = _MISSING):
        r"""Resolve a config string against this Context.

        The form of `value` selects how it is interpreted:

        - `=<expr>`: evaluate the remainder as a Python expression (see `eval`), keeping
          its native type, e.g. `=FileInfo.path.name.upper()` or `=frame_num + 1`.
        - `f"..."` or `F"..."`: evaluate as that f-string, yielding a string. Use this form
          when escape sequences should be processed, e.g. `f"line\n{frame_num}"`.
        - text containing `{...}`: evaluate as a raw f-string template, so fields
          interpolate with full expression power while backslashes stay literal, e.g.
          `C:\Temp\{frame_num}.fits`. Write `{{` or `}}` for a literal brace.
        - anything else: literal text returned verbatim, e.g. `C:\Temp`.

        Args:
            value: The config string to resolve.
            as_type: If given, raise TypeError if the resolved value is not of this type.
            default: The resolved value if a referenced name is absent (see `eval`) or if
                     the input value is None.

        Returns:
            The resolved value: native type for `=<expr>`, a string for the f-string and
            template forms, or the original text for a literal.

        Raises:
            TypeError: if `as_type` is given and the resolved value is not of that type.
        """
        if value is None:
            if default is _MISSING:
                raise TypeError("cannot resolve None without default")

            value = default
        elif value.startswith("="):
            value = self.eval(value[1:], default=default)
        elif value.startswith(_FSTRING_OPENERS):
            value = self.eval(value, default=default)
        elif "{" in value:
            value = self.eval(_raw_fstring_source(value), default=default)

        if as_type is not _MISSING and not isinstance(value, as_type):
            raise TypeError(f"expected {as_type.__name__}, got {type(value).__name__}")

        return value


class ContextSubscription:
    """Subscribe to device keyword updates and produce Context snapshots.

    Monitors one or more keyword types on EntityClients, caching the latest
    value of each.  A ``snapshot`` merges the cached keyword models with an
    optional base context and additional key-value pairs.

    The cached keyword models are stored as-is in the context, preserving
    the strict typing afforded by Keywords.

    Example usage::

        sub = ContextSubscription(mount_client)
        sub.add(AltAzPointing)
        sub.add(RADecPointing)

        await sub.start()
        ctx = sub.snapshot(task.execution.get_context())
        await sub.stop()
    """

    def __init__(self, client: EntityClient):
        self._client = client
        self._add_queue: asyncio.Queue[type[BaseModel]] = asyncio.Queue()
        self._add_task: asyncio.Task | None = None
        self._consumers: list[asyncio.Task] = []
        self._ready = asyncio.Event()
        self._cache = KeywordDict()
        self.cache = MappingProxyType(self._cache)

    def add(self, keyword_type: type[BaseModel]):
        """Register a keyword subscription.

        Args:
            keyword_type: The keyword/model type to subscribe to.
        """
        self._ready.clear()
        self._add_queue.put_nowait(keyword_type)

    async def _subscription_adder(self):
        while True:
            keyword = await self._add_queue.get()

            if self._add_queue.empty():
                self._ready.set()

            # TODO: Need a monitor variant that allows a Queue parameter, then we only need one.
            self._consumers.append(asyncio.create_task(self._consumer(keyword)))

    async def _consumer(self, keyword: type[BaseModel]):
        stream = await self._client.monitor(keyword)

        async for _, data in stream:
            self._cache.set(data)

    async def start(self):
        """Start all subscriptions."""
        if self._add_task:
            raise RuntimeError("ContextSubscription is already running")

        self._add_task = asyncio.create_task(self._subscription_adder())

        if self._add_queue.empty():
            self._ready.set()
        else:
            await self._ready.wait()

    async def stop(self):
        """Cancel all background monitor tasks."""
        if not self._add_task:
            return

        self._add_task.cancel()

        for task in self._consumers:
            task.cancel()

        await asyncio.gather(self._add_task, *self._consumers, return_exceptions=True)

        self._add_task = None
        self._consumers.clear()

    def snapshot(self, into: KeywordDict | None = None) -> Context:
        """Copy the cached Context into a target Context."""
        if into is None:
            into = Context()

        into.update(self._cache)
        return into
