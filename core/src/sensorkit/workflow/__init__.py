# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: F401
"""Observatory sensor structural model, lifecycle and collect orchestration.

`abort` and `dag` know nothing about observatories; `structure` is pure structure
and `views` derives from it; `lifecycle` and `collect` are the two compilers
targeting the dag IR, sharing the dispatch boundary in `ops`; `override` carries
caller-supplied amendments to a compiled graph; `capability` sits above `collect`;
`deployment` is the document surface over all of it.

What this module re-exports is the public API. Anything else is internal,
including every module-private helper.
"""

from sensorkit.workflow.abort import AbortSignal
from sensorkit.workflow.capability import (
    Aliases,
    ByCapability,
    ByRef,
    CapabilityIndex,
    CommandId,
    CommandIdHook,
    CommandRequest,
    DeviceCapabilities,
    ExposureRequest,
    InstrumentEntry,
    KeywordMatch,
    Placement,
    RequestReport,
    RequestResolver,
    RequestStep,
    Scope,
    Selector,
    capabilities_of,
    coalesce,
    matches,
    portability,
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
from sensorkit.workflow.override import Override
from sensorkit.workflow.structure import (
    Assembly,
    Attachment,
    BaseAssembly,
    ClaimKind,
    DeviceNode,
    DeviceRef,
    InstrumentAssembly,
    InstrumentPath,
    InstrumentRole,
    Part,
    SelectorAssembly,
    SensorModel,
    StaticKeywords,
    Trait,
)
from sensorkit.workflow.views import DeviceIndex, InstrumentView, Topology
