from pydantic import BaseModel, model_validator

from sensorkit.auto.agent import AgentConfig
from sensorkit.auto.agent import __name__ as agent_module_name
from sensorkit.common.keyword import KeywordDict
from sensorkit.config.section import declare_config_section
from sensorkit.core.entity import EntityRef
from sensorkit.data.graph import DataGraph

declare_config_section(
    "automation",
    AgentConfig,
    entity_mapper=lambda raw: raw.pop("id", "agent"),
    service_path=agent_module_name,
)


class DataFlowConfig(BaseModel):
    entity: EntityRef
    producer: DataGraph | None = None
    consumer: DataGraph | None = None

    @model_validator(mode="after")
    def _validator(self):
        if (self.producer is None) == (self.consumer is None):
            raise ValueError("Exactly one of 'producer' or 'consumer' must be set")

        return self


declare_config_section(
    "data_flow",
    list[DataFlowConfig],
    entity_mapper=lambda raw: (elem["entity"] for elem in raw),
    model_mapper=lambda obj: (config.producer or config.consumer for config in obj),
)


declare_config_section(
    "config",
    dict[str, KeywordDict],
    entity_mapper=lambda raw: (entity for entity, kwdict in raw.items() for _ in kwdict),
    model_mapper=lambda obj: (model for kwdict in obj.values() for model in kwdict.values()),
)
