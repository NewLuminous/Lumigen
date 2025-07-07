from __future__ import annotations

import pickle
import typing as t
import zlib
import logging

from sqlmesh.core.config.base import BaseConfig, UpdateStrategy
from sqlmesh.utils.pydantic import field_validator
from core.config.connection import (
    ConnectionConfig,
    SeaTunnelConnectorConfig,
    SerializableConnectionConfig,
    connection_config_validator,
)
from core.config.job import (
    job_properties_validator,
    TransformationConfig,
    transformation_pipeline_validator,
)

logger = logging.getLogger(__name__)


_connection_config_field_validator = field_validator(
    "source",
    "sink",
    "executor",
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
    """An object to configure your project.

    Args:
        source: The source connection.
        sink: The sink connection.
        executor: The execution engine connection.
        transformation_pipeline: A list of transformers to be executed in order.
        job_properties: A dictionary of properties to run ETL job.
    """

    source: t.Optional[t.Union[SerializableConnectionConfig, SeaTunnelConnectorConfig]] = None # type: ignore
    sink: t.Optional[t.Union[SerializableConnectionConfig, SeaTunnelConnectorConfig]] = None # type: ignore
    executor: t.Optional[SerializableConnectionConfig] = None # type: ignore
    transformation_pipeline: t.Optional[t.List[TransformationConfig]] = []
    job_properties: t.Dict[str, t.Any] = {}

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "transformation_pipeline": UpdateStrategy.REPLACE,
        "job_properties": UpdateStrategy.KEY_UPDATE,
    }

    _connection_config_validator = _connection_config_field_validator
    _transformation_pipeline_validator = _transformation_pipeline_field_validator
    _job_properties_validator = _job_properties_field_validator
    
    def get_source(self) -> t.Optional[ConnectionConfig]:
        return self.source
    
    def get_sink(self) -> t.Optional[ConnectionConfig]:
        return self.sink
    
    def get_executor(self) -> t.Optional[ConnectionConfig]:
        return self.executor

    @property
    def fingerprint(self) -> str:
        return str(zlib.crc32(pickle.dumps(self.dict(exclude={}))))