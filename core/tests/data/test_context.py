import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.data.context import Context, ContextSubscription


def test_context():
    assert Context() == Context({})
    assert Context({"x": 11}) == Context(x=11)
    assert Context(x=11) == Context((("x", 11,),))


@declare_keyword
class _Temperature(BaseModel):
    celsius: float


@declare_keyword
class _Humidity(BaseModel):
    percent: float


def _make_mock_client(*type_value_pairs):
    """Create a mock EntityClient whose monitor() yields values by type.

    Args:
        *type_value_pairs: Alternating (keyword_type, values_list) pairs,
            or a single (keyword_type, values_list) pair.
    """
    # Build a mapping from keyword type to its value sequence.
    if len(type_value_pairs) == 2 and isinstance(type_value_pairs[1], list):
        # Legacy-style single pair: _make_mock_client(Type, [vals])
        registry = {type_value_pairs[0]: type_value_pairs[1]}
    else:
        # Pairs: _make_mock_client((Type1, [vals1]), (Type2, [vals2]))
        registry = {t: v for t, v in type_value_pairs}

    async def _monitor(model_type):
        async def _gen():
            for v in registry.get(model_type, []):
                yield (None, v)
        return _gen()

    client = MagicMock()
    client.monitor = AsyncMock(side_effect=_monitor)
    return client


@pytest.mark.asyncio
async def test_context_subscription_basic():
    """Subscription caches latest values and snapshot includes them."""
    temp = _Temperature(celsius=22.5)
    client = _make_mock_client(_Temperature, [temp])

    sub = ContextSubscription(client)
    sub.add(_Temperature)

    await sub.start()

    # Allow the monitor task to run.
    await asyncio.sleep(0)

    ctx = sub.snapshot()
    assert ctx.get(_Temperature) == temp

    await sub.stop()


@pytest.mark.asyncio
async def test_context_subscription_snapshot_base_and_kwargs():
    """Snapshot merges base context, cached models, and kwargs."""
    temp = _Temperature(celsius=20.0)
    client = _make_mock_client(_Temperature, [temp])

    sub = ContextSubscription(client)
    sub.add(_Temperature)
    await sub.start()
    await asyncio.sleep(0)

    ctx = KeywordDict(source="sensor1")
    sub.snapshot(ctx)

    assert ctx["source"] == "sensor1"
    assert ctx.get(_Temperature) == temp

    await sub.stop()


@pytest.mark.asyncio
async def test_context_subscription_multiple_keywords():
    """Multiple keyword types are subscribed and cached independently."""
    temp = _Temperature(celsius=15.0)
    hum = _Humidity(percent=65.0)

    client = _make_mock_client((_Temperature, [temp]), (_Humidity, [hum]))

    sub = ContextSubscription(client)
    sub.add(_Temperature)
    sub.add(_Humidity)
    await sub.start()
    await asyncio.sleep(0)

    ctx = sub.snapshot()
    assert ctx.get(_Temperature) == temp
    assert ctx.get(_Humidity) == hum

    await sub.stop()


@pytest.mark.asyncio
async def test_context_subscription_latest_value_wins():
    """When multiple values arrive, the cache holds the latest."""
    temps = [_Temperature(celsius=10.0), _Temperature(celsius=20.0), _Temperature(celsius=30.0)]
    client = _make_mock_client(_Temperature, temps)

    sub = ContextSubscription(client)
    sub.add(_Temperature)
    await sub.start()
    await asyncio.sleep(0)

    ctx = sub.snapshot()
    assert ctx.get(_Temperature) == _Temperature(celsius=30.0)

    await sub.stop()


@pytest.mark.asyncio
async def test_context_subscription_stop_idempotent():
    """Calling stop() when no tasks are running does not raise."""
    client = MagicMock()
    sub = ContextSubscription(client)
    await sub.stop()  # no-op, should not raise
