from importlib.metadata import version as _pkg_version

VERSION = _pkg_version("sensorkit")

from sensorkit.api.bootstrap import connect, import_plugins
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
    on_disable,
    on_enable,
    on_detach,
    on_attach,
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
from sensorkit.core.client import SensorKit, ServiceContext, ServiceRecord, ServiceStatus
from sensorkit.core.controller import ControllerClient, ControllerState, ControllerRef
from sensorkit.core.impl.controller import ControllerImpl
from sensorkit.core.device import (
    Abort,
    CommandDone,
    CommandStarted,
    DeviceClient,
    DeviceCommand,
    DeviceRef,
)
from sensorkit.core.impl.device import DeviceImpl
from sensorkit.core.entity import EntityClient, EntityRef
from sensorkit.core.impl.entity import EntityImpl
from sensorkit.core.state import EventSourcedState
from sensorkit.core.task import (
    CalibrateTask,
    CollectTask,
    ControllerTask,
    InitTask,
    RecoverTask,
    ShutdownTask,
    StandbyTask,
)
from sensorkit.core.trait import Archetype, Trait, declare_archetype, declare_trait
from sensorkit.core.program import (
    OfferInterval,
    ProgramClient,
    ProgramOffering,
    ProgramRef,
    ProgramState,
)
from sensorkit.core.impl.program import ProgramOffers, ProgramImpl
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


# Model imports.
from sensorkit.std.weather import BasicWeather
from sensorkit.models.devices import (
    AxisTargetDistance,
    Binning,
    CameraCapture,
    CameraSensorSize,
    ChangeFocusPosition,
    ChangeRotatorPosition,
    Close,
    Connect,
    Connected,
    Deinit,
    Disable,
    DisableAxis,
    Disconnect,
    Enable,
    EnableAxis,
    Enabled,
    Filter,
    FocusPosition,
    FollowTarget,
    Home,
    Init,
    MountAxis,
    MoveToPark,
    Open,
    Opened,
    RotatorPosition,
    SetBinning,
    SetFilter,
    SetParkPosition,
    SetSyncEnabled,
    SetTemperature,
    SitePosition,
    Stop,
    Target,
    Temperature,
    TemperatureUnit,
)
