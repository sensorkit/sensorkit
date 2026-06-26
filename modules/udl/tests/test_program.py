"""Test UDLProgram request handling and response logic."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import MockCollectRequest

from sensorkit.core.controller import TaskExecutionResult
from sensorkit.core.task import TaskExecution
from sensorkit.udl.models import ResponseStatus, UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram
from sensorkit.udl.task_queue import TaskQueue


@pytest.fixture
def config():
    return UDLConfig(
        entity="udl_program",
        controller="controller1",
        api=UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        ),
    )


@pytest.fixture
def program(config):
    p = UDLProgram()
    p.config = config
    p.program = MagicMock()
    p.program.entity = "udl_program"
    p.program.kv_put_model = AsyncMock()
    p.program.clear_offers = MagicMock()
    p.program.add_offer = MagicMock()
    p.program.publish_offers = AsyncMock()
    p.queue = TaskQueue(p.program)
    p.client = MagicMock()
    p.client.collect_responses = MagicMock()
    p.client.collect_responses.create = AsyncMock()
    return p


class TestHandleCollectRequest:
    @pytest.mark.asyncio
    async def test_new_request_accepted(self, program):
        request = MockCollectRequest.with_tle()
        await program._handle_collect_request(request)

        assert request.id in program.tasks
        assert len(program.queue) == 1

        # Should have sent ACCEPTED response
        program.client.collect_responses.create.assert_awaited_once()
        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["status"] == "ACCEPTED"

    @pytest.mark.asyncio
    async def test_duplicate_request_ignored(self, program):
        request = MockCollectRequest.with_tle()
        await program._handle_collect_request(request)
        await program._handle_collect_request(request)

        # Should only be in queue once
        assert len(program.queue) == 1
        assert program.client.collect_responses.create.await_count == 1

    @pytest.mark.asyncio
    async def test_expired_request_rejected(self, program):
        request = MockCollectRequest.with_tle(
            end_time=datetime.now(UTC) - timedelta(hours=1),
        )
        await program._handle_collect_request(request)

        assert request.id not in program.tasks
        assert len(program.queue) == 0

        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["status"] == "REJECTED"


class TestSendResponse:
    @pytest.mark.asyncio
    async def test_response_uses_config_source(self, program):
        request = MockCollectRequest.with_tle()
        await program._send_response(request, ResponseStatus.ACCEPTED)

        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["source"] == "TEST_SOURCE"

    @pytest.mark.asyncio
    async def test_response_uses_config_id_sensor(self, program):
        request = MockCollectRequest.with_tle()
        await program._send_response(request, ResponseStatus.COLLECTED)

        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["id_sensor"] == "SENSOR-01"

    @pytest.mark.asyncio
    async def test_response_includes_notes(self, program):
        request = MockCollectRequest.with_tle()
        await program._send_response(
            request, ResponseStatus.FAILED, notes="Something went wrong"
        )

        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["notes"] == "Something went wrong"


class TestCollectedResponseActualTimes:
    @pytest.mark.asyncio
    async def test_collected_response_uses_task_execution_times(self, program):
        """COMPLETED carries the TaskExecutionResult window (UDL-formatted, ...Z)."""
        # task_id is a UUID on StandardCollectTask, so the request id must parse.
        request_id = str(uuid.uuid4())
        request = MockCollectRequest.with_tle(id=request_id)
        await program.queue.push_task(request)

        gen = program.generate()
        request_out = await gen.asend(None)
        assert request_out is not None

        result = TaskExecutionResult(
            task_id=uuid.UUID(request_id),
            start_time=datetime(2026, 3, 21, 7, 18, 47, tzinfo=UTC),
            end_time=datetime(2026, 3, 21, 7, 19, 12, tzinfo=UTC),
        )

        # The framework resumes the factory with the minted execution, whose result future the
        # factory awaits. Bind an already-settled future so `await execution` returns the result.
        future = asyncio.get_running_loop().create_future()
        future.set_result(result)
        execution = TaskExecution(
            task=request_out.task,
            task_id=uuid.UUID(request_id),
            controller_id="controller1",
        )
        execution.bind_result(future)

        # Sending the execution resumes past `result = await (yield ...)`; the generator
        # sends COMPLETED and then completes (StopAsyncIteration).
        with pytest.raises(StopAsyncIteration):
            await gen.asend(execution)

        call_kwargs = program.client.collect_responses.create.call_args.kwargs
        assert call_kwargs["status"] == "COMPLETED"
        assert call_kwargs["actual_start_time"] == "2026-03-21T07:18:47.000000Z"
        assert call_kwargs["actual_end_time"] == "2026-03-21T07:19:12.000000Z"


class TestEnvVarFallback:
    def test_env_file_default(self):
        """env_file should default to .env."""
        config = UDLConfig(
            entity="udl_program",
            controller="controller1",
            api=UDLAPIConfig(
                id_sensor="SENSOR-01",
                source="TEST_SOURCE",
            ),
        )
        p = UDLProgram()
        p.config = config
        assert p.config.api.env_file == ".env"

    def test_env_file_custom(self):
        """env_file should be configurable."""
        config = UDLConfig(
            entity="udl_program",
            controller="controller1",
            api=UDLAPIConfig(
                id_sensor="SENSOR-01",
                source="TEST_SOURCE",
                env_file="/opt/sk/.env.udl",
            ),
        )
        p = UDLProgram()
        p.config = config
        assert p.config.api.env_file == "/opt/sk/.env.udl"
