# SPDX-License-Identifier: Apache-2.0
"""Observatory sensor structural model, lifecycle and collect orchestration.

Import order below follows the dependency order: `abort` and `dag` know nothing
about observatories; `structure` is pure structure; `lifecycle` and `collect` are
the two compilers targeting the dag IR; `capability` sits above `collect`;
`deployment` is the document surface over all of it.

`__all__` is the public API. Anything not named here is internal, including every
module-private helper.
"""

from sensorkit.workflow.abort import AbortSignal
from sensorkit.workflow.capability import (
    Bindings,
    ByCapability,
    ByRef,
    DimensionBinding,
    ExposureReq,
    ExternalTask,
    InstrumentEntry,
    RequestResolver,
    Selector,
    TargetCommand,
    TargetSpec,
    TaskStep,
    build_manifest,
    resolve_target_ref,
    select,
)
from sensorkit.workflow.collect import (
    OP_APPLY,
    OP_EXPOSE,
    Collect,
    CollectRunner,
    FramePlan,
    Setting,
    Step,
    SyncPoint,
    compile_collect,
    validate_collect,
    validate_step,
)
from sensorkit.workflow.dag import (
    DagRunner,
    Dispatch,
    Graph,
    GraphBuilder,
    Node,
    NodeOverride,
    NodeResult,
    OnFailure,
    RunReport,
    format_graph,
    topo_order,
)
from sensorkit.workflow.deployment import SensorPlan
from sensorkit.workflow.lifecycle import (
    Entry,
    LifecycleError,
    LifecycleRunner,
    OpSpec,
    Phase,
    PhaseTable,
    Require,
    compile_table,
)
from sensorkit.workflow.ops import (
    STRUCTURAL_MATCHES,
    Match,
    Op,
    OpContext,
    OpHook,
    RunContext,
)
from sensorkit.workflow.override import NodeEffects, Override, resolve_effects
from sensorkit.workflow.structure import (
    Assembly,
    Attachment,
    BaseAssembly,
    ClaimKind,
    DeviceNode,
    DeviceRef,
    InstrumentAssembly,
    InstrumentCapabilities,
    InstrumentPath,
    InstrumentRole,
    Part,
    SelectorAssembly,
    SensorModel,
    Trait,
)
from sensorkit.workflow.views import DeviceIndex, InstrumentView, Topology

__all__ = [
    # abort
    "AbortSignal",
    # dag: the IR and its executor
    "DagRunner", "Dispatch", "Graph", "GraphBuilder", "Node", "NodeOverride",
    "NodeResult", "OnFailure", "RunReport", "format_graph", "topo_order",
    # structure
    "Assembly", "Attachment", "BaseAssembly", "ClaimKind", "DeviceNode",
    "DeviceRef", "InstrumentAssembly", "InstrumentCapabilities",
    "InstrumentPath", "InstrumentRole", "Part", "SelectorAssembly",
    "SensorModel", "Trait",
    # views: derived from the structure
    "DeviceIndex", "InstrumentView", "Topology",
    # ops: the shared dispatch boundary
    "STRUCTURAL_MATCHES", "Match", "Op", "OpContext", "OpHook", "RunContext",
    # override: caller-supplied amendments to a compiled graph
    "NodeEffects", "Override", "resolve_effects",
    # lifecycle
    "Entry", "LifecycleError", "LifecycleRunner", "OpSpec", "Phase",
    "PhaseTable", "Require", "compile_table",
    # collect
    "Collect", "CollectRunner", "FramePlan", "OP_APPLY", "OP_EXPOSE",
    "Setting", "Step", "SyncPoint", "compile_collect", "validate_collect",
    "validate_step",
    # capability
    "Bindings", "ByCapability", "ByRef", "DimensionBinding", "ExposureReq",
    "ExternalTask", "InstrumentEntry", "RequestResolver", "Selector",
    "TargetCommand", "TargetSpec", "TaskStep", "build_manifest",
    "resolve_target_ref", "select",
    # deployment
    "SensorPlan",
]
