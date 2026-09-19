from __future__ import annotations
import typing as t

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig
    from core.engine_adapter.spark.configurator import SparkAdapterConfigurator

from core.adapters import AdapterResolver
from core.engine_adapter.spark.configurators.jdbc import SparkJDBCConfigurator
from core.engine_adapter.spark.configurators.clickhouse import SparkClickHouseConfigurator
from core.engine_adapter.spark.configurators.fs import SparkFileSystemConfigurator
from core.engine_adapter.spark.configurators.kafka import SparkKafkaConfigurator
from core.engine_adapter.spark.configurators.iceberg import SparkIcebergConfigurator


class SparkAdapterResolver(AdapterResolver):
    """Resolves specialized Spark engine adapters for various external systems."""

    # This list acts as our internal plug-in registry.
    # The order can matter if a source could be handled by multiple configurators.
    CONFIGURATORS: t.List[SparkAdapterConfigurator] = [
        SparkClickHouseConfigurator(),
        SparkFileSystemConfigurator(),
        SparkIcebergConfigurator(),
        SparkKafkaConfigurator(),
        SparkJDBCConfigurator(),
    ]

    def resolve(self, external_db_config: ConnectionConfig) -> t.Optional[EngineAdapter]:
        """
        Iterates through the registered configurators to find one that can handle
        the external database connection and returns a specialized adapter.
        """
        for configurator in self.CONFIGURATORS:
            adapter = configurator.configure(self.executor_config, external_db_config)
            if adapter:
                # `adapter.dialect` is the runtime execution/SQL-generation dialect (e.g., 'spark').
                # `adapter.DIALECT` identifies the underlying external source system (e.g., 'postgres', 'oracle')
                # used for auditing, state tracking, and source-level metadata validation.
                adapter.DIALECT = external_db_config.create_engine_adapter().dialect or adapter.DIALECT
                return adapter
            
        return None