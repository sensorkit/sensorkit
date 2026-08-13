# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from _contextvars import ContextVar
from typing import Callable, ClassVar, override

from loguru import logger

from sensorkit.backend.request import CallContext
from sensorkit.core.device import (
    DeviceEnableState,
    DeviceEnableStateRequest,
    DeviceInterface,
    DeviceState,
    set_enable_state_request,
)
from sensorkit.core.entity import (
    CommandRequestMessage,
    CommandResult,
    DeviceDetails,
    EntityInfo,
)
from sensorkit.core.impl.entity import EntityImpl


class DeviceImpl(EntityImpl, DeviceInterface):
    """Helper for implementing server-side functionality of a Device."""

    current: ClassVar[ContextVar[DeviceImpl | None]] = ContextVar("current_device", default=None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._enable_hooks: list[Callable[[], None]] = []
        self._disable_hooks: list[Callable[[], None]] = []
        self._published_keywords: set[str] = set()  # _handlers is on EntityImpl now

    def declare_published_keyword(self, keyword_id: str):
        """Declare that this device publishes a keyword."""
        self._published_keywords.add(keyword_id)

    @override
    def on_enable(self, func: Callable[[], None]):
        self._enable_hooks.append(func)
        return func

    @override
    def on_disable(self, func: Callable[[], None]):
        self._disable_hooks.append(func)
        return func

    @override
    async def init_impl(self):
        self._state = await DeviceState.recover_or_init(
            self,
            enable_state=DeviceEnableState(enabled=True),
        )

    @override
    async def attach_impl(self):
        # EntityImpl.attach_impl wires the command request to self._command_request (the gated
        # override below, via MRO).
        await super().attach_impl()

        if self._state.enable_state.enabled:
            await self._call_with_context(self._enable_hooks)

        await self.handle_request(set_enable_state_request, self._set_enable_state)

    async def _set_enable_state(self, request: DeviceEnableStateRequest):
        if request.enable == self._state.enable_state.enabled:
            return

        await self._state.update(self, DeviceEnableState(enabled=request.enable))

        if request.enable:
            await self._call_with_context(self._enable_hooks)
        else:
            # TODO: Interrupt all ongoing commands.

            await self._call_with_context(self._disable_hooks)

    @override
    async def _command_request(
        self,
        message: CommandRequestMessage,
        call: CallContext[None, CommandResult],
    ):
        # Devices gate commands on their enable state; everything else is the shared entity path.
        if not self._state.enable_state.enabled:
            logger.warning(f"Rejecting {message.command.command_id} command: Device is disabled")
            call.reject(response=None)
            return

        await super()._command_request(message, call)

    @override
    def entity_info(self) -> EntityInfo:
        return EntityInfo(
            entity_type="device",
            details=DeviceDetails(
                supported_commands=frozenset(self._handlers.keys()),
                published_keywords=frozenset(self._published_keywords),
            ),
        )
