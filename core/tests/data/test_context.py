# SPDX-License-Identifier: Apache-2.0
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


# --- Context.eval -----------------------------------------------------------------------


def test_eval_expression():
    ctx = Context(frame_num=7)
    assert ctx.eval("frame_num + 1") == 8
    assert ctx.eval("frame_num * 2") == 14


def test_eval_keyword_attribute():
    ctx = Context(_Temperature(celsius=22.5))
    assert ctx.eval("_Temperature.celsius") == 22.5


def test_eval_missing_name_raises():
    with pytest.raises(NameError):
        Context().eval("nope")


def test_eval_missing_name_returns_default():
    assert Context().eval("nope", default=None) is None
    assert Context().eval("nope", default="fallback") == "fallback"


def test_eval_non_name_error_propagates_past_default():
    # A missing *attribute* on a present value is a real error, not an absent keyword,
    # so it propagates even when a default is supplied.
    ctx = Context(frame_num=7)
    with pytest.raises(AttributeError):
        ctx.eval("frame_num.no_such_attr", default=None)


# --- Context.resolve (Grammar A: =expr | f"..." | literal) ------------------------------


def test_resolve_literal_verbatim():
    ctx = Context(frame_num=7)
    # No prefix and no "{...}" -> returned exactly as-is: no interpolation, no escapes.
    assert ctx.resolve("/Temp/SimulatedSensor") == "/Temp/SimulatedSensor"
    assert ctx.resolve(r"C:\Temp\new") == r"C:\Temp\new"


def test_resolve_expression_keeps_native_type():
    ctx = Context(frame_num=7)
    result = ctx.resolve("=frame_num + 1")
    assert result == 8 and isinstance(result, int)


def test_resolve_expression_keyword_method():
    ctx = Context(_Temperature(celsius=22.5))
    assert ctx.resolve("=str(_Temperature.celsius)") == "22.5"


def test_resolve_fstring_returns_string():
    ctx = Context(frame_num=7)
    assert ctx.resolve('f"{frame_num}.fits"') == "7.fits"
    assert ctx.resolve('f"{frame_num:03d}.fits"') == "007.fits"
    assert ctx.resolve("f'{frame_num}'") == "7"


@pytest.mark.parametrize("prefix", ["f", "F"])
def test_resolve_fstring_prefix_variants(prefix):
    ctx = Context(frame_num=7)
    assert ctx.resolve(f'{prefix}"{{frame_num}}.fits"') == "7.fits"


def test_resolve_default_forwarded_to_eval():
    ctx = Context()
    assert ctx.resolve("=missing_kw", default=None) is None
    assert ctx.resolve('f"{missing_kw}"', default="fallback") == "fallback"
    # Literals never reference keywords, so default is irrelevant there.
    assert ctx.resolve("plain", default="fallback") == "plain"


def test_resolve_missing_name_raises_without_default():
    with pytest.raises(NameError):
        Context().resolve("=missing_kw")
    with pytest.raises(NameError):
        Context().resolve('f"{missing_kw}"')


def test_resolve_brace_template_interpolates():
    # An un-prefixed value containing "{...}" is a raw-f-string template.
    ctx = Context(frame_num=7, program_name="survey_north")
    assert ctx.resolve("{program_name}/{frame_num}.fits") == "survey_north/7.fits"
    # Full expression power inside fields, not just bare names.
    assert ctx.resolve("{frame_num + 1:03d}.fits") == "008.fits"


def test_resolve_brace_template_keeps_backslashes():
    # The headline win: a backslash path that *also* interpolates stays literal.
    ctx = Context(frame_num=7)
    assert ctx.resolve(r"C:\Temp\{frame_num}.fits") == r"C:\Temp\7.fits"
    # Backslash escapes are NOT processed in template mode.
    assert ctx.resolve(r"a\t{frame_num}") == "a\\t7"


def test_resolve_brace_template_literal_brace():
    # Doubled braces produce a literal brace.
    assert Context().resolve("{{literal}}") == "{literal}"


def test_resolve_brace_template_default():
    ctx = Context()
    # Missing field with a default is suppressed; without one it raises NameError.
    assert ctx.resolve("{missing}", default="fallback") == "fallback"
    with pytest.raises(NameError):
        ctx.resolve("{missing}")


def test_resolve_gate_passes_brace_free_values_verbatim():
    # The "{"-gate is transparent: brace-free values (incl. trailing backslash, which a
    # raw f-string could not represent) are returned exactly, never compiled.
    ctx = Context()
    assert ctx.resolve("C:\\Temp\\") == "C:\\Temp\\"
    assert ctx.resolve('ends-with-quote"') == 'ends-with-quote"'


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("{frame_num}\\", "7\\"),              # template ending in a single backslash
        ("C:\\d\\{frame_num}\\", "C:\\d\\7\\"),  # backslash path template ending in "\"
        ('{frame_num}"', '7"'),                # template ending in a quote
        ('say "{frame_num}"', 'say "7"'),      # quotes around an interpolation
        ('{frame_num}"""', '7"""'),            # trailing triple-quote (peeled)
    ],
)
def test_resolve_brace_template_quote_and_backslash_tails(value, expected):
    # Trailing quotes/backslashes are handled, not rejected.
    assert Context(frame_num=7).resolve(value) == expected


def test_resolve_brace_template_embedded_triple_quote_raises():
    # A mid-value triple-quote is unsupported (implausible in real config) and errors.
    with pytest.raises(SyntaxError):
        Context(frame_num=7).resolve('a"""b{frame_num}')


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
