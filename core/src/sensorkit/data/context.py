from __future__ import annotations

import ast
import asyncio
import builtins
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING

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


class Context(KeywordDict):
    """A KeywordDict for storing data context."""

    def eval(self, expr: str):
        """Evaluate a Python expression using this Context as the namespace."""
        tree = ast.parse(expr, mode="eval")
        # TODO: Lock down evaluation by pruning the AST.
        return eval(compile(tree, "<string>", mode="eval"), _EVAL_GLOBALS, self)


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
        ctx = sub.snapshot(task.get_context())
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
