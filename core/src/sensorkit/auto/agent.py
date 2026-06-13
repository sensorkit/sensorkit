from __future__ import annotations

import asyncio
from typing import Annotated, Any, Literal

from loguru import logger
from pydantic import AfterValidator, BaseModel, Field

import sensorkit.api as sk
from sensorkit.auto.operator import ControllerConfig, VirtualOperator
from sensorkit.auto.scheduler import Schedule
from sensorkit.core.task import TaskContexts


@sk.declare_keyword
class ElectionVotes(BaseModel):
    """Published election keyword: per-source, per-controller vote state for observability."""
    votes: dict[str, dict[str, bool | None]] = Field(default_factory=dict)


def _set_controller_config_names(configs: dict[str, ControllerConfig]):
    for name, config in configs.items():
        config.name = name

    return configs


ControllerConfigMap = Annotated[
    dict[str, ControllerConfig],
    AfterValidator(_set_controller_config_names),
]


class FirstRunConfig(BaseModel):
    """Initial state to assume on the first run before any persisted agent state exists."""
    operate_all: bool = False
    enable_scheduling: bool = True


class AgentConfig(BaseModel):
    """Top-level agent service configuration."""
    first_run: FirstRunConfig = Field(default_factory=FirstRunConfig)
    controllers: ControllerConfigMap = Field(default_factory=dict)
    contexts: TaskContexts = Field(default_factory=TaskContexts)

    def model_post_init(self, __context: Any):
        for config in self.controllers.values():
            config.propagate_config(self.contexts)


class Capabilities(BaseModel):
    """Deprecated agent capability descriptor published to the KV store."""
    type: Literal["agent"] = "agent"
    controllers: ControllerConfigMap


class AgentControllerInfo(BaseModel):
    """Per-controller operating state: control gate, elected state, and demand override."""
    control_enabled: bool
    elected_state: bool | None
    demand_override: bool | None

    @classmethod
    def default(cls):
        """Return a default AgentControllerInfo with control enabled and no overrides."""
        return cls(control_enabled=True, elected_state=None, demand_override=None)


class AgentOperatingState(sk.Event):
    """Event capturing the agent's global control gate and per-controller operating info."""
    global_control_enabled: bool
    controllers: dict[str, AgentControllerInfo] = Field(default_factory=dict)

    def derive_for_request(self, req: AgentConfigureRequest):
        """Return a new AgentOperatingState reflecting the changes in *req*."""
        return AgentOperatingState(
            global_control_enabled=(
                req.global_control_enabled
                if req.global_control_enabled is not None
                else self.global_control_enabled
            ),
            controllers={
                name: AgentControllerInfo(
                    control_enabled=req.controller_control_enabled.get(name, info.control_enabled),
                    elected_state=info.elected_state,
                    demand_override=req.controller_demand_override.get(name, info.demand_override),
                )
                for name, info in self.controllers.items()
            },
        )

    def derive_for_status(self, *, votes: dict[str, bool]):
        """Return a new AgentOperatingState updated with the latest election *votes*."""
        return AgentOperatingState(
            global_control_enabled=self.global_control_enabled,
            controllers={
                name: info.model_copy(update=dict(elected_state=votes.get(name)))
                for name, info in self.controllers.items()
            },
        )

    def derive_for_config_change(self, *, configs: ControllerConfigMap):
        """Return an updated state reconciled against *configs*, or None if no change is needed."""
        if set(self.controllers) == set(configs):
            return None

        controllers = {
            name: info
            for name, info in self.controllers.items()
            if name in configs
        }

        for name in configs:
            if name not in controllers:
                controllers[name] = AgentControllerInfo.default()

        return AgentOperatingState(
            global_control_enabled=self.global_control_enabled,
            controllers=controllers,
        )


class AgentSchedulerState(sk.Event):
    """Event capturing the scheduler gate, program exclusions, and current schedule snapshots."""
    scheduling_enabled: bool
    excluded_programs: set[str] = Field(default_factory=set)
    schedule: dict[str, Schedule] = Field(default_factory=dict)

    def derive_for_request(self, req: AgentConfigureRequest):
        """Return a new AgentSchedulerState reflecting the scheduler changes in *req*."""
        return AgentSchedulerState(
            scheduling_enabled=(
                req.scheduling_enabled
                if req.scheduling_enabled is not None
                else self.scheduling_enabled
            ),
            excluded_programs=(
                self.excluded_programs.union(req.add_program_exclusions).difference(
                    req.remove_program_exclusions
                )
            ),
            schedule=self.schedule,
        )

    def derive_for_status(self, *, schedule: dict[str, Schedule]):
        """Return a copy of this state updated with the latest *schedule* snapshots."""
        return self.model_copy(update=dict(schedule=schedule))


class AgentState(sk.EventSourcedState):
    """Persisted agent state combining operating and scheduler sub-states."""
    operating_state: AgentOperatingState
    scheduler_state: AgentSchedulerState

    async def apply_to_operator(self, operator: VirtualOperator, configs: ControllerConfigMap):
        """Push this state into the VirtualOperator: programs, overrides, and lifecycle gates."""
        async with self.update_lock:
            if self.scheduler_state.scheduling_enabled:
                await operator.programs.global_enable()
            else:
                await operator.programs.global_disable()

            for program in operator.programs.all_programs:
                if program in self.scheduler_state.excluded_programs:
                    await operator.programs.disable(program)
                else:
                    await operator.programs.enable(program)

            for controller in configs:
                info = self.operating_state.controllers.get(controller)

                if info:
                    operator.election.vote(
                        source="override", subject=controller, vote=info.demand_override
                    )

                lifecycle = operator.drivers[controller].lifecycle

                if self.operating_state.global_control_enabled:
                    if info is None or info.control_enabled:
                        lifecycle.enable()
                        continue

                lifecycle.disable()


class AgentConfigureRequest(BaseModel):
    """Request payload for the agent configure RPC, specifying partial state changes."""
    global_control_enabled: bool | None = None
    controller_control_enabled: dict[str, bool] = Field(default_factory=dict)
    controller_demand_override: dict[str, bool | None] = Field(default_factory=dict)
    scheduling_enabled: bool | None = None
    add_program_exclusions: set[str] = Field(default_factory=set)
    remove_program_exclusions: set[str] = Field(default_factory=set)


agent_configure_request = sk.Request.define(
    "configure",
    payload=AgentConfigureRequest,
    response=AgentState,
)


# TODO: Move this to a generic entity impl. Want a core feature to collapse single-entity services
#       into a single entity first.
@sk.service_entrypoint(version=sk.VERSION)
async def agent_service(service: sk.Service):
    # Register the service.
    await service.register()

    # Read config.
    config = await service.context.kv_get_model(AgentConfig)

    # Recover state.
    state = await AgentState.recover_or_init(
        service.context,
        operating_state=AgentOperatingState(global_control_enabled=config.first_run.operate_all),
        scheduler_state=AgentSchedulerState(scheduling_enabled=config.first_run.enable_scheduling),
    )

    # Make sure our state structure is in agreement with configuration.
    if update := state.operating_state.derive_for_config_change(configs=config.controllers):
        logger.debug("controllers manifest was updated due to config change")
        await state.update(service.context, update)

    # Create the virtual operator and apply our initial state to it.
    operator = VirtualOperator(config.controllers.values())
    await state.apply_to_operator(operator, config.controllers)

    # Start the virtual operator.
    with service.context.enter_context():
        await operator.start(client=service.client, task_group=service.context.task_group)

    # Handle configuration requests.
    # FIXME: asyncio.Lock does not guarantee FIFO wakeup order, so this usage could result in
    #        scrambled concurrent request calls. Also, the number of waiters is unbounded. The
    #        request API should provide a "serialized" mode that internally utilizes a bounded
    #        Queue to guarantee FIFO ordering and put an upper bound on the number of waiting
    #        requests.
    _configure_lock = asyncio.Lock()

    async def _agent_configure_request(request: AgentConfigureRequest):
        async with _configure_lock:
            # Apply the configuration into the current state.
            await state.update(
                service.context,
                state.scheduler_state.derive_for_request(request),
                state.operating_state.derive_for_request(request),
            )

            # Configure the virtual operator to reflect the new state.
            await state.apply_to_operator(operator, config.controllers)

        return state

    await service.context.handle_request(agent_configure_request, _agent_configure_request)

    async def publish():
        while True:
            await service.context.publish(
                ElectionVotes(votes=operator.election._votes)
            )

            await state.update(
                service.context,
                state.scheduler_state.derive_for_status(
                    schedule={
                        name: driver.scheduler.get_schedule()
                        for name, driver in operator.drivers.items()
                    }
                ),
                state.operating_state.derive_for_status(votes=operator.election.evaluate()),
            )

            await asyncio.sleep(1)

    _publish_task = asyncio.create_task(publish())

    await service.context.kv_put_model(Capabilities(
        controllers=config.controllers
    ))

    # Run the service.
    await service.run()
