# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: F401 E402
from importlib.metadata import version as _pkg_version

try:
    VERSION = _pkg_version("sensorkit")
except Exception:
    VERSION = "0.0.0"

from sensorkit.api.bootstrap import connect, import_modules, load_config, set_config_location
from sensorkit.api.declarative import (
    DeclaredController,
    DeclaredDevice,
    DeclaredEntity,
    DeclaredProgram,
    Service,
    command_handler,
    decl_for_instance,
    declare_controller,
    declare_device,
    declare_entity,
    declare_program,
    entity_for_instance,
    on_attach,
    on_detach,
    on_disable,
    on_enable,
    task_factory,
    task_handler,
)
from sensorkit.api.entrypoint import service_entrypoint
from sensorkit.backend.base import (
    Backend,
    BackendImpl,
    Entity,
    KVEntry,
    KVError,
    RevisionError,
    SpecialProperty,
    Subject,
)
from sensorkit.backend.event import Event, UnknownEvent
from sensorkit.backend.request import CallContext, CallError, ExtendedResponse, Request
from sensorkit.common.keyword import Keyword, KeywordDict, declare_keyword
from sensorkit.config import config_json_schema, declare_config_section
from sensorkit.core.client import SensorKit, ServiceContext, ServiceRecord, ServiceStatus
from sensorkit.core.controller import ControllerClient, ControllerRef, ControllerState
from sensorkit.core.device import DeviceClient, DeviceRef
from sensorkit.core.entity import (
    Abort,
    CommandDone,
    CommandStarted,
    DeviceCommand,
    EntityClient,
    EntityRef,
)
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.impl.program import ProgramImpl, ProgramOffers
from sensorkit.core.program import (
    OfferInterval,
    ProgramClient,
    ProgramOffering,
    ProgramRef,
    ProgramState,
)
from sensorkit.core.state import EventSourcedState
from sensorkit.core.task import (
    CalibrateTask,
    CollectTask,
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
    Task,
    TaskExecution,
    TaskInfo,
    TaskSubmission,
)
from sensorkit.core.trait import Archetype, Trait, declare_archetype, declare_trait
from sensorkit.data.context import Context, ContextSubscription
from sensorkit.data.graph import DataGraph


def entity(obj=None) -> EntityImpl | None:
    """Return the entity in the given or current execution context."""
    if obj:
        return entity_for_instance(obj)
    else:
        return EntityImpl.current.get()


def program(obj=None) -> ProgramImpl | None:
    """Return the program entity in the given or current execution context."""
    if obj:
        return entity_for_instance(obj)
    else:
        return ProgramImpl.current.get()


def controller(obj=None) -> ControllerImpl | None:
    """Return the controller entity in the given or current execution context."""
    if obj:
        return entity_for_instance(obj)
    else:
        return ControllerImpl.current.get()


def device(obj=None) -> DeviceImpl | None:
    """Return the device entity in the given or current execution context."""
    if obj:
        return entity_for_instance(obj)
    else:
        return DeviceImpl.current.get()
