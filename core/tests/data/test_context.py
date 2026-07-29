# SPDX-License-Identifier: Apache-2.0
import asyncio

import pytest
import pytest_asyncio
from pydantic import BaseModel

from sensorkit.common.keyword import KeywordDict, declare_keyword
from sensorkit.data.context import Context, ContextSubscription

# --- Composite keyword expansion --------------------------------------------------------


def test_set_composite_expands():
    @declare_keyword
    class Part(BaseModel):
        x: int

    @declare_keyword
    class Whole(BaseModel):
        part: Part

        def composed_keywords(self):
            yield self.part

    ctx = Context()
    whole = Whole(part=Part(x=42))

    # Setting the composite also makes what it composes available under that keyword's own key.
    ctx.set(whole)
    assert ctx[Whole] is whole
    assert ctx[Part] is whole.part

    # Writes follow argument order, so a part given after the composite replaces the composed
    # one, and a part given before it does not.
    other = Part(x=7)
    ctx.set(whole, other)
    assert ctx[Part] is other

    ctx.set(other, whole)
    assert ctx[Part] is whole.part


def test_set_composite_expands_nested():
    @declare_keyword
    class Leaf(BaseModel):
        x: int

    @declare_keyword
    class Branch(BaseModel):
        leaf: Leaf

        def composed_keywords(self):
            yield self.leaf

    @declare_keyword
    class Trunk(BaseModel):
        branch: Branch

        def composed_keywords(self):
            yield self.branch

    ctx = Context()
    trunk = Trunk(branch=Branch(leaf=Leaf(x=42)))

    # Expansion is transitive: the leaf is reachable only through the branch, but setting the
    # trunk must still make it available under its own key.
    ctx.set(trunk)
    assert ctx[Trunk] is trunk
    assert ctx[Branch] is trunk.branch
    assert ctx[Leaf] is trunk.branch.leaf


def test_set_composite_expands_on_cycle():
    @declare_keyword
    class Ouroboros(BaseModel):
        x: int
        other: "Ouroboros | None" = None

        def composed_keywords(self):
            if self.other is not None:
                yield self.other

    ctx = Context()
    snake = Ouroboros(x=42)
    snake.other = snake

    # A keyword that composes itself terminates rather than recurring forever.
    ctx.set(snake)
    assert ctx[Ouroboros] is snake


def test_composite_expands_from_keyword_dict_source():
    @declare_keyword
    class Piece(BaseModel):
        x: int

    @declare_keyword
    class Bundle(BaseModel):
        piece: Piece

        def composed_keywords(self):
            yield self.piece

    bundle = Bundle(piece=Piece(x=42))

    # A bare KeywordDict does not expand, so its composed keyword is absent...
    stored = KeywordDict()
    stored.set(bundle)
    assert stored.get(Piece) is None

    # ...but building a Context over it expands on the way in.
    ctx = Context(stored)
    assert ctx[Bundle] is bundle
    assert ctx[Piece] is bundle.piece


def test_composite_expands_via_update():
    @declare_keyword
    class Sub(BaseModel):
        x: int

    @declare_keyword
    class Super(BaseModel):
        sub: Sub

        def composed_keywords(self):
            yield self.sub

    sup = Super(sub=Sub(x=42))

    source = KeywordDict()
    source.set(sup)

    ctx = Context()
    ctx.update(source)
    assert ctx[Super] is sup
    assert ctx[Sub] is sup.sub


# --- Context.eval -----------------------------------------------------------------------


def test_eval_keyword_attribute():
    ctx = Context(SampleTemperature(celsius=22.5))
    assert ctx.eval("SampleTemperature.celsius") == 22.5


def test_eval_keyword_expression():
    ctx = Context(SampleFrameInfo(frame_num=7))
    assert ctx.eval("SampleFrameInfo.frame_num + 1") == 8
    assert ctx.eval("SampleFrameInfo.frame_num * 2") == 14


def test_eval_missing_name_raises():
    with pytest.raises(NameError):
        Context().eval("nope")


def test_eval_missing_name_returns_default():
    assert Context().eval("nope", default=None) is None
    assert Context().eval("nope", default="fallback") == "fallback"


def test_eval_non_name_error_propagates_past_default():
    # A missing *attribute* on a present value is a real error, not an absent keyword,
    # so it propagates even when a default is supplied.
    ctx = Context(SampleFrameInfo(frame_num=7))
    with pytest.raises(AttributeError):
        ctx.eval("SampleFrameInfo.no_such_attr", default=None)


# --- Context.resolve (Grammar A: =expr | f"..." | literal) ------------------------------


def test_resolve_literal_verbatim():
    # No prefix and no "{...}" -> returned exactly as-is: no interpolation, no escapes.
    assert Context().resolve("/Temp/SimulatedSensor") == "/Temp/SimulatedSensor"
    assert Context().resolve(r"C:\Temp\new") == r"C:\Temp\new"


def test_resolve_expression_keeps_native_type():
    ctx = Context(SampleFrameInfo(frame_num=7))
    result = ctx.resolve("=SampleFrameInfo.frame_num + 1")
    assert result == 8 and isinstance(result, int)


def test_resolve_expression_keyword_method():
    ctx = Context(SampleTemperature(celsius=22.5))
    assert ctx.resolve("=str(SampleTemperature.celsius)") == "22.5"


def test_resolve_fstring_returns_string():
    ctx = Context(SampleFrameInfo(frame_num=7))
    assert ctx.resolve('f"{SampleFrameInfo.frame_num}.fits"') == "7.fits"
    assert ctx.resolve('f"{SampleFrameInfo.frame_num:03d}.fits"') == "007.fits"
    assert ctx.resolve("f'{SampleFrameInfo.frame_num}'") == "7"


@pytest.mark.parametrize("prefix", ["f", "F"])
def test_resolve_fstring_prefix_variants(prefix):
    ctx = Context(SampleFrameInfo(frame_num=7))
    assert ctx.resolve(f'{prefix}"{{SampleFrameInfo.frame_num}}.fits"') == "7.fits"


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
    ctx = Context(SampleFrameInfo(frame_num=7), SampleTemperature(celsius=22.5))
    assert ctx.resolve("{SampleTemperature.celsius}/{SampleFrameInfo.frame_num}.fits") == "22.5/7.fits"
    # Full expression power inside fields, not just bare names.
    assert ctx.resolve("{SampleFrameInfo.frame_num + 1:03d}.fits") == "008.fits"


def test_resolve_brace_template_keeps_backslashes():
    # The headline win: a backslash path that *also* interpolates stays literal.
    ctx = Context(SampleFrameInfo(frame_num=7))
    assert ctx.resolve(r"C:\Temp\{SampleFrameInfo.frame_num}.fits") == r"C:\Temp\7.fits"
    # Backslash escapes are NOT processed in template mode.
    assert ctx.resolve(r"a\t{SampleFrameInfo.frame_num}") == "a\\t7"


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
        ("{SampleFrameInfo.frame_num}\\", "7\\"),              # template ending in a backslash
        ("C:\\d\\{SampleFrameInfo.frame_num}\\", "C:\\d\\7\\"),  # backslash path ending in "\"
        ('{SampleFrameInfo.frame_num}"', '7"'),                # template ending in a quote
        ('say "{SampleFrameInfo.frame_num}"', 'say "7"'),      # quotes around an interpolation
        ('{SampleFrameInfo.frame_num}"""', '7"""'),            # trailing triple-quote (peeled)
    ],
)
def test_resolve_brace_template_quote_and_backslash_tails(value, expected):
    # Trailing quotes/backslashes are handled, not rejected.
    assert Context(SampleFrameInfo(frame_num=7)).resolve(value) == expected


def test_resolve_brace_template_embedded_triple_quote_raises():
    # A mid-value triple-quote is unsupported (implausible in real config) and errors.
    with pytest.raises(SyntaxError):
        Context(SampleFrameInfo(frame_num=7)).resolve('a"""b{SampleFrameInfo.frame_num}')


@declare_keyword
class SampleTemperature(BaseModel):
    celsius: float


@declare_keyword
class SampleHumidity(BaseModel):
    percent: float


@declare_keyword
class SampleFrameInfo(BaseModel):
    frame_num: int


@pytest_asyncio.fixture
async def subscription(kit, device_impl):
    """A ContextSubscription reading the keywords `device_impl` publishes."""
    sub = ContextSubscription(kit.entity(device_impl.entity))

    yield sub

    await sub.stop()


async def cached(sub, keyword, expected, timeout=2.0):
    """Wait until the subscription's snapshot holds `expected` for the keyword, and return it.

    Raises:
        TimeoutError: if that value never arrives.
    """
    async with asyncio.timeout(timeout):
        while sub.snapshot().get(keyword) != expected:
            await asyncio.sleep(0.01)

    return expected


@pytest.mark.asyncio
async def test_context_subscription_basic(subscription, device_impl):
    """Subscription caches latest values and snapshot includes them."""
    temp = SampleTemperature(celsius=22.5)
    subscription.add(SampleTemperature)
    await subscription.start()

    await device_impl.publish(temp)

    assert await cached(subscription, SampleTemperature, temp)


@pytest.mark.asyncio
async def test_context_subscription_snapshot_merges_base(subscription, device_impl):
    """Snapshot merges a given base context with the cached keyword models."""
    temp = SampleTemperature(celsius=20.0)
    subscription.add(SampleTemperature)
    await subscription.start()

    await device_impl.publish(temp)
    await cached(subscription, SampleTemperature, temp)

    base = Context(SampleHumidity(percent=50.0))
    ctx = subscription.snapshot(base)

    assert ctx is base
    assert ctx.get(SampleHumidity) == SampleHumidity(percent=50.0)
    assert ctx.get(SampleTemperature) == temp


@pytest.mark.asyncio
async def test_context_subscription_multiple_keywords(subscription, device_impl):
    """Multiple keyword types are subscribed and cached independently."""
    temp = SampleTemperature(celsius=15.0)
    hum = SampleHumidity(percent=65.0)

    subscription.add(SampleTemperature)
    subscription.add(SampleHumidity)
    await subscription.start()

    await device_impl.publish(temp)
    await device_impl.publish(hum)

    assert await cached(subscription, SampleTemperature, temp)
    assert await cached(subscription, SampleHumidity, hum)


@pytest.mark.asyncio
async def test_context_subscription_latest_value_wins(subscription, device_impl):
    """When multiple values arrive, the cache holds the latest."""
    subscription.add(SampleTemperature)
    await subscription.start()

    for celsius in (10.0, 20.0, 30.0):
        await device_impl.publish(SampleTemperature(celsius=celsius))

    assert await cached(subscription, SampleTemperature, SampleTemperature(celsius=30.0))


@pytest.mark.asyncio
async def test_context_subscription_stop_idempotent(kit, device_impl):
    """Calling stop() when no tasks are running does not raise."""
    sub = ContextSubscription(kit.entity(device_impl.entity))
    await sub.stop()  # no-op, should not raise
