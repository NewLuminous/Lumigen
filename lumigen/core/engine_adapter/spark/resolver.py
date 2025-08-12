from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, KafkaConnectionConfig

from core.adapters import AdapterResolver
from core.engine_adapter import SparkJDBCEngineAdapter, SparkKafkaEngineAdapter


class SparkAdapterResolver(AdapterResolver):
    """Resolves specialized Spark engine adapters for various external systems."""

    def resolve(self, external_db_config: ConnectionConfig) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ == "kafka":
            from engines.spark.spark_kafka import connection
            
            kafka_config: KafkaConnectionConfig = external_db_config
            conn_with_kwargs = partial(
                connection,
                **{
                    **self.executor_config._static_connection_kwargs,
                    "bootstrap_servers": kafka_config.bootstrap_servers,
                    "group_id": kafka_config.group_id,
                    "starting_mode": kafka_config.starting_mode,
                    "kafka_config": kafka_config.config,
                    **{k: v for k, v in self.executor_config.dict().items() if k in self.executor_config._connection_kwargs_keys},
                },
            )
            return SparkKafkaEngineAdapter(conn_with_kwargs, dialect=kafka_config.type_)

        else: # Default to trying JDBC for other database types
            from engines.spark.spark_jdbc import connection, SparkJDBCCursor

            if external_db_config.type_ in SparkJDBCCursor.SUPPORTED_SOURCES:
                external_engine_adapter = external_db_config.create_engine_adapter()
                conn_with_kwargs = partial(
                    connection,
                    **{
                        **self.executor_config._static_connection_kwargs,
                        "connection_config": external_db_config,
                        **{k: v for k, v in self.executor_config.dict().items() if k in self.executor_config._connection_kwargs_keys},
                    },
                )
                adapter = SparkJDBCEngineAdapter(conn_with_kwargs, dialect=external_engine_adapter.dialect)
                adapter.SCHEMA_DIFFER = external_engine_adapter.SCHEMA_DIFFER
                return adapter
        
        return None