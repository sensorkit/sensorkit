# SPDX-License-Identifier: Apache-2.0
"""Test UDLProgram request handling and response logic."""

import asyncio
import gzip
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from unifieddatalibrary.types import CollectRequestFull

from sensorkit.core.controller import TaskExecutionResult
from sensorkit.core.task import TaskExecution
from sensorkit.udl.models import CollectConfig, ResponseStatus, UDLAPIConfig, UDLConfig
from sensorkit.udl.program import UDLProgram, UDLState
from sensorkit.udl.task_queue import TaskQueue

from .fakes import FakeUDLClient, poll_once, tle_request


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


async def minted_camera_params(program, request):
    """Camera params of the task the factory mints for one queued request."""
    await program.queue.push_task(request)
    gen = program.generate()

    try:
        minted = await gen.asend(None)
    finally:
        await gen.aclose()

    return minted.task.camera_params


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

        (task_dict,) = json.loads(gzip.decompress(restored.state.pending_collect_requests))
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


class TestUnbuildableTask:
    @pytest.mark.asyncio
    async def test_invalid_task_parameters_fail(self, program):
        program.config.collect = CollectConfig(binning_from_extra="binning")
        request = tle_request(binning="wide")
        await program.queue.push_task(request)

        gen = program.generate()
        assert await gen.asend(None) is None

        with pytest.raises(StopAsyncIteration):
            await gen.asend(None)

        assert len(program.queue) == 0
        assert request.id in program.state.resolved_collect_requests
        assert program.client.collect_responses.statuses() == ["FAILED"]


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
        await poll_once(program)

        (query,) = program.client.collect_requests.queries
        assert query["extra_query"]["idSensor"] == "SENSOR-01"
        assert "origSensorId" not in query["extra_query"]

    @pytest.mark.asyncio
    async def test_orig_sensor_id_filter(self, program):
        program.config.api.poll_filter = "origSensorId"

        await poll_once(program)

        (query,) = program.client.collect_requests.queries
        assert query["extra_query"]["origSensorId"] == "SENSOR-01"
        assert "idSensor" not in query["extra_query"]

    @pytest.mark.asyncio
    async def test_polled_requests_are_accepted(self, program):
        """A page of results is handled, not just fetched."""
        program.client.collect_requests.page.items = [tle_request(id="polled-1")]

        await poll_once(program)

        assert "polled-1" in program.tasks
        assert program.client.collect_responses.statuses() == ["ACCEPTED"]


class TestNonstandardBinning:
    """Binning a tasker attaches to a CollectRequest outside the UDL schema."""

    def test_pair_parses_from_a_config_list(self):
        collect = CollectConfig.model_validate(
            {"binning_from_extra": ["binningHoriz", "binningVert"]}
        )

        assert collect.binning_from_extra == ("binningHoriz", "binningVert")

    @pytest.mark.asyncio
    async def test_configured_binning_used_by_default(self, program):
        program.config.collect = CollectConfig(binning=1)

        params = await minted_camera_params(program, tle_request(binningHoriz=2, binningVert=3))

        assert (params.binning_x, params.binning_y) == (1, 1)

    @pytest.mark.asyncio
    async def test_one_name_binds_both_axes(self, program):
        program.config.collect = CollectConfig(binning=1, binning_from_extra="binning")

        params = await minted_camera_params(program, tle_request(binning=2))

        assert (params.binning_x, params.binning_y) == (2, 2)

    @pytest.mark.asyncio
    async def test_a_pair_names_each_axis(self, program):
        program.config.collect = CollectConfig(
            binning=1, binning_from_extra=("binningHoriz", "binningVert")
        )

        params = await minted_camera_params(program, tle_request(binningHoriz=2, binningVert=3))

        assert (params.binning_x, params.binning_y) == (2, 3)

    @pytest.mark.asyncio
    async def test_configured_binning_when_request_omits(self, program):
        program.config.collect = CollectConfig(
            binning=1, binning_from_extra=("binningHoriz", "binningVert")
        )

        params = await minted_camera_params(program, tle_request())

        assert (params.binning_x, params.binning_y) == (1, 1)

    @pytest.mark.asyncio
    async def test_members_survive_request_handling(self, program):
        """Members outside the schema ride through the validation the program applies."""
        await program._handle_collect_request(tle_request(binningHoriz=2, binningVert=3))

        queued = await program.queue.peek_task()
        assert queued.model_extra.get("binningHoriz") == 2
        assert queued.model_extra.get("binningVert") == 3
