# SPDX-License-Identifier: Apache-2.0
"""Test UDLProgram request handling and response logic."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from unifieddatalibrary.types import CollectRequestFull

from .fakes import FakeUDLClient, tle_request
from sensorkit.core.controller import TaskExecutionResult
from sensorkit.core.task import TaskExecution
from sensorkit.udl.models import ResponseStatus, UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram, UDLState
from sensorkit.udl.task_queue import TaskQueue


@pytest.fixture
def config():
    return UDLConfig(
        controller="controller1",
        api=UDLAPIConfig(
            id_sensor="SENSOR-01",
            source="TEST_SOURCE",
        ),
    )


@pytest.fixture
def program(config, program_impl):
    """A UDLProgram wired the way program_init() leaves it, minus the background loops."""
    p = UDLProgram()
    p.config = config
    p.program = program_impl
    p.queue = TaskQueue(program_impl)
    p.client = FakeUDLClient()
    return p


def responses(program):
    """Every CollectResponse the program posted, oldest first."""
    return program.client.collect_responses.created


class TestHandleCollectRequest:
    @pytest.mark.asyncio
    async def test_new_request_accepted(self, program):
        request = tle_request()
        await program._handle_collect_request(request)

        assert request.id in program.tasks
        assert len(program.queue) == 1
        assert program.client.collect_responses.statuses() == ["ACCEPTED"]

    @pytest.mark.asyncio
    async def test_duplicate_request_ignored(self, program):
        request = tle_request()
        await program._handle_collect_request(request)
        await program._handle_collect_request(request)

        assert len(program.queue) == 1
        assert program.client.collect_responses.statuses() == ["ACCEPTED"]

    @pytest.mark.asyncio
    async def test_expired_request_rejected(self, program):
        request = tle_request(end_time=datetime.now(UTC) - timedelta(hours=1))
        await program._handle_collect_request(request)

        assert request.id not in program.tasks
        assert len(program.queue) == 0
        assert program.client.collect_responses.statuses() == ["REJECTED"]


class TestSendResponse:
    @pytest.mark.asyncio
    async def test_response_uses_config_source(self, program):
        await program._send_response(tle_request(), ResponseStatus.ACCEPTED)
        assert responses(program)[-1]["source"] == "TEST_SOURCE"

    @pytest.mark.asyncio
    async def test_response_uses_config_id_sensor(self, program):
        await program._send_response(tle_request(), ResponseStatus.COLLECTED)
        assert responses(program)[-1]["id_sensor"] == "SENSOR-01"

    @pytest.mark.asyncio
    async def test_response_includes_notes(self, program):
        await program._send_response(
            tle_request(), ResponseStatus.FAILED, notes="Something went wrong"
        )
        assert responses(program)[-1]["notes"] == "Something went wrong"


class TestPersistedState:
    """Pending tasks survive a restart, so they must round-trip through the KV store."""

    @pytest.mark.asyncio
    async def test_queued_requests_restore(self, program, program_impl):
        request = tle_request(id=str(uuid.uuid4()))
        await program.queue.push_task(request)
        await program._save_state()

        restored = UDLProgram()
        restored.program = program_impl
        await restored._restore_state()

        (task_dict,) = restored.state.pending_tasks
        revived = CollectRequestFull.model_validate(task_dict)
        assert revived.id == request.id
        assert revived.num_frames == request.num_frames
        assert revived.elset.line1 == request.elset.line1

    @pytest.mark.asyncio
    async def test_missing_state_starts_empty(self, program_impl):
        restored = UDLProgram()
        restored.program = program_impl
        await restored._restore_state()

        assert restored.state == UDLState()


class TestCollectedResponseActualTimes:
    @pytest.mark.asyncio
    async def test_collected_response_uses_task_execution_times(self, program):
        """COLLECTED carries the TaskExecutionResult window (UDL-formatted, ...Z)."""
        # task_id is a UUID on StandardCollectTask, so the request id must parse.
        request_id = str(uuid.uuid4())
        await program.queue.push_task(tle_request(id=request_id))

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
        # sends COLLECTED and then completes (StopAsyncIteration).
        with pytest.raises(StopAsyncIteration):
            await gen.asend(execution)

        sent = responses(program)[-1]
        assert sent["status"] == "COLLECTED"
        assert sent["actual_start_time"] == "2026-03-21T07:18:47.000000Z"
        assert sent["actual_end_time"] == "2026-03-21T07:19:12.000000Z"


class TestEnvVarFallback:
    def test_env_file_default(self, config):
        assert config.api.env_file == ".env"

    def test_env_file_custom(self):
        config = UDLConfig(
            controller="controller1",
            api=UDLAPIConfig(
                id_sensor="SENSOR-01",
                source="TEST_SOURCE",
                env_file="/opt/sk/.env.udl",
            ),
        )
        assert config.api.env_file == "/opt/sk/.env.udl"


class TestPollFilter:
    @pytest.mark.asyncio
    async def test_default_polls_by_id_sensor(self, program):
        await program._poll_collect_requests()

        (query,) = program.client.collect_requests.queries
        assert query["extra_query"]["idSensor"] == "SENSOR-01"
        assert "origSensorId" not in query["extra_query"]

    @pytest.mark.asyncio
    async def test_orig_sensor_id_filter(self, program):
        program.config.api.poll_filter = "orig_sensor_id"

        await program._poll_collect_requests()

        (query,) = program.client.collect_requests.queries
        assert query["extra_query"]["origSensorId"] == "SENSOR-01"
        assert "idSensor" not in query["extra_query"]

    @pytest.mark.asyncio
    async def test_polled_requests_are_accepted(self, program):
        """A page of results is handled, not just fetched."""
        program.client.collect_requests.page.items = [tle_request(id="polled-1")]

        await program._poll_collect_requests()

        assert "polled-1" in program.tasks
        assert program.client.collect_responses.statuses() == ["ACCEPTED"]
