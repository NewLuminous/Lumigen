from __future__ import annotations

import pickle
import typing as t
import zlib
from pydantic import field_validator, model_validator
import logging

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from core.config.connection import (
    ConnectionConfig,
    SeaTunnelConnectorConfig,
    SerializableConnectionConfig,
    connection_config_validator,
    SUPPORTED_AUDIT_ENGINES,
)
from core.config.job import (
    job_properties_validator,
    TransformationConfig,
    transformation_pipeline_validator,
    DataLoadMode,
)

logger = logging.getLogger(__name__)


_connection_config_field_validator = field_validator(
    "source",
    "sink",
    "executor",
    "audit_store",
    mode="before",
    check_fields=False,
)(connection_config_validator)

_transformation_pipeline_field_validator = field_validator(
    "transformation_pipeline",
    mode="before",
    check_fields=False,
)(transformation_pipeline_validator)

_job_properties_field_validator = field_validator(
    "job_properties",
    mode="before",
    check_fields=False,
)(job_properties_validator)


class Config(BaseConfig):
    """An object to configure the project.

    Args:
        source: The source connection from which to read data.
        sink: The destination connection to which data will be written.
        executor: The execution engine connection (e.g., Spark) to use for processing.
        audit_store: The database connection for the tool's operational audit and metadata store.
        transformation_pipeline: A list of transformers to be executed in order.
        job_properties: A dictionary of properties to configure the ETL job's behavior.
    """

    source: t.Optional[t.Union[SerializableConnectionConfig, SeaTunnelConnectorConfig]] = None # type: ignore
    sink: t.Optional[t.Union[SerializableConnectionConfig, SeaTunnelConnectorConfig]] = None # type: ignore
    executor: t.Optional[SerializableConnectionConfig] = None # type: ignore
    audit_store: t.Optional[SerializableConnectionConfig] = None # type: ignore
    transformation_pipeline: t.Optional[t.List[TransformationConfig]] = []
    job_properties: t.Dict[str, t.Any] = {}
    env: t.Dict[str, str] = {}

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "transformation_pipeline": UpdateStrategy.REPLACE,
        "job_properties": UpdateStrategy.KEY_UPDATE,
        "env": UpdateStrategy.KEY_UPDATE,
    }

    _connection_config_validator = _connection_config_field_validator
    _transformation_pipeline_validator = _transformation_pipeline_field_validator
    _job_properties_validator = _job_properties_field_validator

    @field_validator("audit_store", mode="after")
    @classmethod
    def _validate_audit_store_type(cls, v: t.Optional[ConnectionConfig]) -> t.Optional[ConnectionConfig]:
        if v is not None and not v.is_supported_for_audit:
            supported_types_str = ", ".join(sorted(list(SUPPORTED_AUDIT_ENGINES)))
            # Construct the improved, dynamic error message
            raise ValueError(
                f"Invalid connection type '{v.type_}' for 'audit_store'. "
                f"Supported types are: {supported_types_str}."
            )
        return v
    
    @model_validator(mode='after')
    def _set_source_defaults(self) -> "Config":
        """
        Sets default values for job properties based on the source type.
        This runs before the requirement validation.
        """
        if self.source:
            if self.source.type_ == "kafka":
                self.job_properties["source_watermark"] = "offset"
        return self
    
    @model_validator(mode='after')
    def _validate_incremental_requirements(self) -> "Config":
        load_mode_str = self.job_properties.get("data_load_mode", "FULL")
        if DataLoadMode(load_mode_str) != DataLoadMode.INCREMENTAL:
            return self

        if self.audit_store is None:
            raise ValueError(
                f"An 'audit_store' connection must be configured when `data_load_mode` is '{DataLoadMode.INCREMENTAL}'."
            )
        
        if not self.job_properties.get("source_watermark"):
            raise ValueError(
                f"'source_watermark' must be configured when `data_load_mode` is '{DataLoadMode.INCREMENTAL}'."
            )

        return self
    
    def get_source(self) -> t.Optional[ConnectionConfig]:
        return self.source
    
    def get_sink(self) -> t.Optional[ConnectionConfig]:
        return self.sink
    
    def get_executor(self) -> t.Optional[ConnectionConfig]:
        return self.executor
    
    def get_audit_store(self) -> t.Optional[ConnectionConfig]:
        return self.audit_store
    
    @property
    def job_id(self) -> t.Optional[str]:
        return self.job_properties.get("job_id")

    @property
    def fingerprint(self) -> str:
        return str(zlib.crc32(pickle.dumps(self.dict(exclude={"audit_store"}))))