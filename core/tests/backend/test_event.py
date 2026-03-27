from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from sensorkit.backend.base import KVError
from sensorkit.backend.event import Event, EventMultiplexer, UnknownEvent
from sensorkit.core.state import EventSourcedState


class SampleEvent1(Event):
    value: int


class SampleEvent2(Event):
    name: str


class SampleEvent3(Event):
    flag: bool


class MockState(EventSourcedState):
    event1: SampleEvent1
    event2: SampleEvent2


@pytest.mark.asyncio
async def test_event_validation():
    event = SampleEvent1(value=100)
    data = event.model_dump()

    # Test validate_any
    validated = Event.model_validate(data)
    assert validated == event
    assert isinstance(validated, SampleEvent1)

    # Test validate_any_json
    validated_json = Event.model_validate_json(event.model_dump_json().encode())
    assert validated_json == event
    assert isinstance(validated_json, SampleEvent1)

    # Events should be immutable
    with pytest.raises(ValidationError, match="frozen"):
        validated.value = 200


@pytest.mark.asyncio
async def test_unknown_event_validation():
    data = {"event_model": "NonExistentEvent", "foo": "bar"}
    validated = Event.model_validate(data)
    assert validated.event_model == "NonExistentEvent"
    assert isinstance(validated, UnknownEvent)
    # UnknownEvent has extra="allow"
    assert validated.foo == "bar"


@pytest.mark.asyncio
async def test_event_multiplexer():
    mux = EventMultiplexer()

    event = SampleEvent1(value=42)
    event_json = event.model_dump_json().encode()

    with mux.event_queue(SampleEvent1) as q:
        await mux.parse_event(event_json)
        received = await q.get()
        assert received == event
        assert isinstance(received, SampleEvent1)


@pytest.mark.asyncio
async def test_event_multiplexer_all_events():
    mux = EventMultiplexer()

    event1 = SampleEvent1(value=42)
    event2 = SampleEvent2(name="hello")

    with mux.all_events() as q:
        await mux.parse_event(event1.model_dump_json().encode())
        await mux.parse_event(event2.model_dump_json().encode())

        received1 = await q.get()
        received2 = await q.get()

        assert received1 == event1
        assert received2 == event2


@pytest.mark.asyncio
async def test_event_sourced_state_inheritance():
    class SubMockState(MockState):
        event3: SampleEvent3

    state = SubMockState(
        event1=SampleEvent1(value=1),
        event2=SampleEvent2(name="test"),
        event3=SampleEvent3(flag=True),
    )

    assert SampleEvent1 in state._event_fields
    assert SampleEvent2 in state._event_fields
    assert SampleEvent3 in state._event_fields
    assert state._event_fields[SampleEvent1] == "event1"
    assert state._event_fields[SampleEvent2] == "event2"
    assert state._event_fields[SampleEvent3] == "event3"

    # Ensure that the base class fields are not affected by the subclass
    assert SampleEvent3 not in MockState._event_fields
    assert MockState._event_fields is not SubMockState._event_fields


@pytest.mark.asyncio
async def test_event_sourced_state_introspection():
    state = MockState(event1=SampleEvent1(value=1), event2=SampleEvent2(name="test"))

    # Check that introspection happened and fields are correctly mapped
    assert SampleEvent1 in state._event_fields
    assert state._event_fields[SampleEvent1] == "event1"
    assert SampleEvent2 in state._event_fields
    assert state._event_fields[SampleEvent2] == "event2"


@pytest.mark.asyncio
async def test_event_sourced_state_duplicate_error():
    # Attempting to define a state with duplicate event types should raise an error
    # We need to do this in a way that triggers introspection

    with pytest.raises(RuntimeError, match="Duplicate event type SampleEvent1"):

        class DuplicateEventState(EventSourcedState):
            e1: SampleEvent1
            e2: SampleEvent1

        DuplicateEventState(e1=SampleEvent1(value=1), e2=SampleEvent1(value=2))


@pytest.mark.asyncio
async def test_event_sourced_state_update():
    state = MockState(event1=SampleEvent1(value=1), event2=SampleEvent2(name="test"))

    entity = AsyncMock()

    new_event = SampleEvent1(value=2)
    await state.update(entity, new_event)

    assert state.event1 == new_event
    entity.emit_event.assert_awaited_once_with(new_event)
    entity.kv_put_model.assert_awaited_once_with(state)


@pytest.mark.asyncio
async def test_event_sourced_state_update_no_publish():
    state = MockState(event1=SampleEvent1(value=1), event2=SampleEvent2(name="test"))

    entity = AsyncMock()

    new_event = SampleEvent1(value=2)
    await state.update(entity, new_event, publish_state=False)

    assert state.event1 == new_event
    entity.emit_event.assert_awaited_once_with(new_event)
    entity.kv_put_model.assert_not_called()


@pytest.mark.asyncio
async def test_event_sourced_state_update_invalid_event():
    state = MockState(event1=SampleEvent1(value=1), event2=SampleEvent2(name="test"))

    entity = AsyncMock()

    class AnotherUnknownEvent(Event):
        pass

    # This should fail because AnotherUnknownEvent is not in MockState's fields
    with pytest.raises(KeyError):
        await state.update(entity, AnotherUnknownEvent())


@pytest.mark.asyncio
async def test_event_sourced_state_recover():
    state = MockState(event1=SampleEvent1(value=1), event2=SampleEvent2(name="test"))

    entity = AsyncMock()
    entity.kv_get_model.return_value = state

    recovered = await MockState.recover(entity)

    assert recovered.event1 == state.event1
    assert recovered.event2 == state.event2
    entity.kv_get_model.assert_awaited_once_with(MockState)

    entity.reset_mock()
    entity.kv_get_model.side_effect = KVError("Key not found")

    new = await MockState.recover_or_init(
        entity, event1=SampleEvent1(value=2), event2=SampleEvent2(name="init")
    )

    assert new.event1.value == 2
    assert new.event2.name == "init"
    entity.kv_put_model.assert_awaited_once_with(new)
