from __future__ import annotations

import asyncio
from _contextvars import ContextVar
from typing import Callable, ClassVar, override

from loguru import logger
from pydantic import ValidationError

from sensorkit.backend.request import CallContext
from sensorkit.core.device import (
    CommandDone,
    CommandHandlerCallback,
    CommandRequestMessage,
    CommandResult,
    CommandStarted,
    DeviceCommand,
    DeviceEnableState,
    DeviceEnableStateRequest,
    DeviceInterface,
    DeviceState,
    run_command_request,
    set_enable_state_request,
)
from sensorkit.core.entity import DeviceDetails, EntityInfo
from sensorkit.core.impl.entity import EntityImpl


class DeviceImpl(EntityImpl, DeviceInterface):
    """Helper for implementing server-side functionality of a Device."""

    current: ClassVar[ContextVar[DeviceImpl | None]] = ContextVar("current_device", default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._enable_hooks: set[Callable[[], None]] = set()
        self._disable_hooks: set[Callable[[], None]] = set()
        self._handlers: dict[str, CommandHandlerCallback] = {}
        self._published_keywords: set[str] = set()

    def declare_published_keyword(self, keyword_id: str):
        """Declare that this device publishes a keyword."""
        self._published_keywords.add(keyword_id)

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
        self._state = await DeviceState.recover_or_init(
            self,
            enable_state=DeviceEnableState(enabled=True),
        )

        if self._state.enable_state.enabled:
            await self._call_with_context(self._enable_hooks)

        # Set up the command request handler.
        await self.handle_request(set_enable_state_request, self._set_enable_state)
        await self.handle_request(run_command_request, self._command_request)

    async def _set_enable_state(self, request: DeviceEnableStateRequest):
        if request.enable == self._state.enable_state.enabled:
            return

        await self._state.update(self, DeviceEnableState(enabled=request.enable))

        if request.enable:
            await self._call_with_context(self._enable_hooks)
        else:
            # TODO: Interrupt all ongoing commands.

            await self._call_with_context(self._disable_hooks)

    async def _command_request(
        self,
        message: CommandRequestMessage,
        call: CallContext[None, CommandResult],
    ):
        command_id = message.command.command_id

        # Reject if we aren't ready.
        if not self._state.enable_state.enabled:
            logger.warning(f"Rejecting {command_id} command: Device is disabled")
            call.reject(response=None)
            return

        # Look for a handler for this command ID.
        if command_id not in self._handlers:
            logger.warning(f"Rejecting unhandled command: {command_id}")
            call.reject(response=None)
            return

        # Accept the command and invoke the configured handler func.
        call.accept(response=None)
        handler_func = self._handlers[command_id]
        success = False

        # Emit the command start event.
        logger.debug(f"Incoming {command_id} Command: {call.call_id}")
        await self.emit_event(CommandStarted(command_id=command_id, call_id=call.call_id))

        try:
            # Invoke the registered handler function in a new task and store a reference.
            with self.enter_context():
                task = asyncio.create_task(handler_func(message.command))

            # Wait for the handler to return.
            await call.progress_from_task(task, cadence=6.0, ttl=10.0)
            success = True
        except ValidationError:
            logger.exception(f"Incoming {command_id} Command failed validation")
            raise
        except asyncio.CancelledError:
            logger.exception(f"Execution of {command_id} Command cancelled")
            raise
        except Exception:
            logger.exception(f"Error invoking {command_id} Command handler")
            raise
        else:
            # The command completed successfully, so return the result including the handler func
            # return value.
            await call.succeed(result=CommandResult(data=task.result()))
        finally:
            # Emit the command end event.
            await self.emit_event(
                CommandDone(
                    command_id=command_id,
                    call_id=call.call_id,
                    success=success,
                )
            )

    @override
    def command_handler(self, command_type: type[DeviceCommand]):
        key = command_type.model_tag()

        def decorator(func: CommandHandlerCallback):
            self._handlers[key] = func
            return func

        return decorator

    @override
    async def publish_entity_info(self) -> EntityInfo:
        info = EntityInfo(
            entity_type="device",
            details=DeviceDetails(
                supported_commands=frozenset(self._handlers.keys()),
                published_keywords=frozenset(self._published_keywords),
            ),
        )
        await self.kv_put_model(info)
        return info
