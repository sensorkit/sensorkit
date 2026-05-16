from __future__ import annotations

import asyncio
import contextlib
from _contextvars import ContextVar
from datetime import UTC, datetime
from typing import Callable, ClassVar, Collection, Mapping, override

from loguru import logger

from sensorkit.backend.base import Entity
from sensorkit.backend.request import CallContext
from sensorkit.common.keyword import KeywordDict
from sensorkit.core.controller import (
    AbortRequestMessage,
    AbortResponseMessage,
    ControllerDevice,
    ControllerEnableState,
    ControllerEnableStateRequest,
    ControllerInterface,
    ControllerOperatingState,
    ControllerState,
    ExecuteRequestMessage,
    ExecuteResponseMessage,
    InternalControllerState,
    TaskExecutionResult,
    TaskExecutionState,
    TaskFinishInfo,
    TaskHandlerCallback,
    abort_task_request,
    execute_task_request,
    set_enable_state_request,
)
from sensorkit.core.device import DeviceClient
from sensorkit.core.entity import ControllerDetails, EntityInfo
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.task import ControllerTask
from sensorkit.data.context import Context, ContextSubscription


class ControllerDeviceMap(Mapping[str, ControllerDevice]):
    """Ordered mapping of device names to their ControllerDevice instances, created on demand."""

    def __init__(self):
        self._dict: dict[str, ControllerDevice] = {}

    def get_or_add(self, name: str, *, get_client_func: Callable[[str], DeviceClient]):
        """Return the ControllerDevice for the given name, creating it if it does not yet exist."""
        if name not in self._dict:
            client = get_client_func(name)
            self._dict[name] = ControllerDevice(client, ContextSubscription(client))

        return self._dict[name]

    def __getitem__(self, key, /):
        return self._dict[key]

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)


class ControllerImpl(EntityImpl, ControllerInterface):
    """Helper for implementing server-side functionality of a Controller."""

    current: ClassVar[ContextVar[ControllerImpl | None]] = ContextVar("current_controller", default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._enable_hooks: set[Callable[[], None]] = set()
        self._disable_hooks: set[Callable[[], None]] = set()
        self._task_handlers: dict[type[ControllerTask], TaskHandlerCallback] = {}
        self._task_asyncio: asyncio.Task | None = None
        self._devices = ControllerDeviceMap()

    @override
    def task_running(self):
        return bool(self._task_asyncio)

    @override
    def on_enable(self, func: Callable[[], None]):
        self._enable_hooks.add(func)
        return func

    @override
    def on_disable(self, func: Callable[[], None]):
        self._disable_hooks.add(func)
        return func

    @override
    async def init_impl(self):
        self._state = await ControllerState.recover_or_init(
            self,
            enable_state=ControllerEnableState(enabled=True),
            operating_state=ControllerOperatingState(current=InternalControllerState.UNKNOWN),
            execution_state=TaskExecutionState(task=None),
        )

        if self._state.enable_state.enabled:
            await self._call_with_context(self._enable_hooks)

        await self.handle_request(set_enable_state_request, self._set_enable_state)
        await self.handle_request(abort_task_request, self._abort_request)
        await self.handle_request(execute_task_request, self._execute_request)

    async def update_task_context(self, context: dict):
        """Publish progress context for the in-flight task.

        Updates TaskExecutionState.context so observers can render live progress
        info — current frame number, sub-step name, etc. — without having to introspect
        the device-level keyword stream.

        No-op if no task is currently executing. Replaces (not merges) the
        published context dict; callers should pass a complete snapshot.
        """
        current = self._state.execution_state
        if not current.executing or current.task is None:
            return

        await self._state.update(
            self,
            TaskExecutionState(
                executing=current.executing,
                aborting=current.aborting,
                task=current.task,
                context=context,
            ),
        )

    @override
    def use_device(self, name: str, *, subscribe: list[type] | None = None) -> DeviceClient:
        """Register a controlled device and return a client for it.

        Registers a device that this controller interacts with. Optionally subscribes to
        specific keyword types published by the device. Subscribed keywords are automatically
        cached and made available in contexts built via ``build_context()``.

        Note:
            Subscriptions remain inactive until ``start_device_subscriptions()`` is called.
            When using the declarative API, this occurs automatically after initialization.

        Args:
            name: Entity identifier for the device (e.g., "mount", "camera").
            subscribe: Optional list of keyword model types (e.g., Pydantic models) to
                subscribe to. Each subscribed keyword will be monitored, and its latest
                value cached for inclusion in contexts.

        Returns:
            A client instance for interacting with the registered device.
                Access the subscription object via ``get_device(name).subscription`` to
                retrieve cached values directly.

        Raises:
            KeyError: If attempting to access a device via ``get_device(name)`` that
                hasn't been registered with ``use_device()``.

        Examples:
            Basic device registration without subscriptions:

            >>> camera = self.use_device("camera")

            Register device and subscribe to specific keyword types:

            >>> mount = self.use_device("mount", subscribe=[AltAzPointing, RADecPointing])

            Access cached subscription values from the ControlledDevice instance:

            >>> pointing = self.get_device("mount").get(AltAzPointing)
        """
        device = self._devices.get_or_add(
            name,
            get_client_func=lambda _: self.sensorkit().device(Entity((name,))),
        )

        if subscribe:
            for kw_type in subscribe:
                device.subscription.add(kw_type)

        return device.client

    @override
    def get_device(self, name: str) -> ControllerDevice:
        return self._devices[name]

    @override
    def all_devices(self) -> Collection[ControllerDevice]:
        return self._devices.values()

    @override
    async def start_device_subscriptions(self):
        """Start all keyword subscriptions registered via ``use_device``.

        This is called automatically by the declarative API after init
        callbacks have run.  It may also be called manually if the
        controller is not using the declarative API.
        """
        for device in self._devices.values():
            await device.subscription.start()

    @override
    async def stop_device_subscriptions(self):
        """Stop all keyword subscriptions registered via ``use_device``.

        Called automatically by the declarative API before deinit
        callbacks run.
        """
        for device in self._devices.values():
            await device.subscription.stop()

    @override
    def build_context(
        self,
        base: KeywordDict | None = None,
        **kwargs,
    ) -> Context:
        """Build a Context from all device keyword subscriptions.

        Merges cached keyword models from every subscription registered
        via ``use_device(subscribe=...)`` with an optional *base* context
        and additional key-value pairs.

        Args:
            base: Optional base context (e.g. from a task) to include.
            **kwargs: Additional literal key-value pairs to include.

        Returns:
            A new :class:`Context` containing (in precedence order,
            highest-last): *base*, cached keyword models, and *kwargs*.
        """
        ctx = Context(base)

        for device in self._devices.values():
            device.subscription.snapshot(into=ctx)

        ctx.update(kwargs)
        return ctx

    async def _set_enable_state(self, request: ControllerEnableStateRequest):
        if request.enable == self._state.enable_state.enabled:
            return

        await self._state.update(self, ControllerEnableState(enabled=request.enable))

        if request.enable:
            await self._call_with_context(self._enable_hooks)
        else:
            #TODO: Abort. Need a nice way of calling a local "loopback" request.
            # await self.call_loopback(abort_task_request, AbortRequestMessage(task_id=None))

            await self._call_with_context(self._disable_hooks)

    async def _abort_request(
        self,
        requested: AbortRequestMessage,
        call: CallContext[AbortResponseMessage, None],
    ):
        # Cannot abort if no task is running.
        if not self.task_running():
            call.reject(response=AbortResponseMessage(aborting=False, task_id=None))
            return

        aio_task = self._task_asyncio
        task_id = self._state.execution_state.task.task_id

        # If given, ensure the task_id matches what is running.
        if requested.task_id and task_id != requested.task_id:
            call.reject(response=AbortResponseMessage(aborting=False, task_id=None))
            return

        # Send an accept response and update status indicating we are about to abort.
        logger.debug(f"aborting running task {task_id}")
        call.accept(response=AbortResponseMessage(aborting=True, task_id=task_id))
        await self._state.update(
            self,
            TaskExecutionState(
                executing=True,
                aborting=True,
                task=self._state.execution_state.task
            ),
        )

        # Abort the asyncio.Task running the SensorKit Task.
        aio_task.cancel("Aborted by remote request")

        # Wait for the task to die.
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await call.progress_from_task(aio_task, cadence=5, ttl=10)

        if aio_task.done():
            # Success.
            await call.succeed(result=None)
        else:
            await call.fail()

    async def _execute_request(
        self,
        msg: ExecuteRequestMessage,
        call: CallContext[ExecuteResponseMessage, TaskExecutionResult],
    ):
        task = msg.task

        # Reject if we aren't ready.
        if not self._state.enable_state.enabled:
            logger.warning(f"Rejecting {type(task)} task: Controller is disabled")
            call.reject(response=ExecuteResponseMessage(task_id=task.task_id))
            return

        # Reject if there's already a Task in work.
        if not msg.interrupt and self.task_running():
            logger.warning(f"Rejecting {type(task)} task: Task already in progress")
            call.reject(response=ExecuteResponseMessage(task_id=task.task_id))
            return

        # Ensure we have a handler defined.
        if type(task) not in self._task_handlers:
            logger.warning(f"Rejecting {type(task)} task: No handler registered")
            call.reject(response=ExecuteResponseMessage(task_id=task.task_id))
            return

        # Accept the task execution request.
        call.accept(response=ExecuteResponseMessage(task_id=task.task_id))
        aio_task: asyncio.Task | None = None

        try:
            # Interrupt the current task, if there is one.
            if self._task_asyncio:
                logger.info(f"Interrupting {self._state.execution_state.task.task_type} task")
                self._task_asyncio.cancel("Interrupted")

                with contextlib.suppress(asyncio.CancelledError):
                    await call.progress_from_task(
                        self._task_asyncio,
                        cadence=6.0,
                        ttl=10.0,
                    )

            # Execute the task.
            logger.info(f"Executing {task.task_type} task")
            start_time = datetime.now(UTC)

            with self.enter_context():
                aio_task = asyncio.create_task(self._execute_task(task))

            self._task_asyncio = aio_task

            await call.progress_from_task(
                self._task_asyncio,
                cadence=6.0,
                ttl=10.0,
            )

            end_time = datetime.now(UTC)
        except asyncio.CancelledError:
            logger.warning(f"Execution of {task.task_type} task cancelled")
            await call.fail()
        except Exception as e:
            logger.error(f"Error executing {task.task_type} task")
            logger.opt(exception=e).debug(f"{task.task_type} ({task.task_id}) failed")
            await call.fail()
        else:
            logger.info(f"Finished {task.task_type} task")
            await call.succeed(
                result=TaskExecutionResult(
                    task_id=task.task_id,
                    start_time=start_time,
                    end_time=end_time,
                )
            )
        finally:
            # Clear the task object only if it wasn't overwritten in the meantime.
            if self._task_asyncio is aio_task:
                self._task_asyncio = None

                if not aio_task.done():
                    aio_task.cancel()

                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await aio_task

    async def _execute_task(self, task: ControllerTask):
        logger.debug(f"execution begun {task=}")
        finish_info: TaskFinishInfo | None = None

        # Emit the execution start event.
        await self._state.update(
            self,
            TaskExecutionState(executing=True, task=task),
            self._state.operating_state.derive(target=task.target_state()),
        )

        try:
            # Find the user-supplied task handler callback for this task type.
            handler = self._task_handlers.get(type(task))
            assert handler

            # Run the task handler.
            await handler(task)
        except asyncio.CancelledError:
            finish_info = TaskFinishInfo(aborted=True)
            raise
        except Exception as e:
            finish_info = TaskFinishInfo(error=str(e))
            raise
        else:
            finish_info = TaskFinishInfo()
        finally:
            assert finish_info is not None

            try:
                await self._state.update(
                    self,
                    TaskExecutionState(finished=finish_info, task=task),
                    self._state.operating_state.derive(
                        current=ts if (ts := task.target_state()) is not None else ControllerOperatingState.NO_CHANGE,
                        target=None,
                    ),
                )
            except Exception:
                logger.exception("Failed to update execution state after task completion")

    @override
    def task_handler[T: ControllerTask](self, task_model: type[T]):
        def decorator(func: TaskHandlerCallback[T]):
            if task_model in self._task_handlers:
                raise RuntimeError("multiple handlers for task type")

            self._task_handlers[task_model] = func
            return func

        return decorator

    @override
    async def publish_entity_info(self) -> EntityInfo:
        info = EntityInfo(
            entity_type="controller",
            details=ControllerDetails(
                supported_tasks=[t.__name__ for t in self._task_handlers.keys()],
                controlled_devices=list(self._devices.keys()),
            ),
        )
        await self.kv_put_model(info)
        return info

    @override
    async def set_internal_state(self, state: InternalControllerState):
        await self._state.update(self, self._state.operating_state.derive(current=state))
