from __future__ import annotations
import typing as t
from functools import partial
import logging

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, KafkaConnectionConfig

from core.engine_adapter import SparkJDBCEngineAdapter, SparkKafkaEngineAdapter
from core.engine_context import SQLContextManager
from utils.errors import ConfigError


def validate_connections(connections: t.List[t.Optional[ConnectionConfig]]):
    """
    Validates that a connection can be established for each configuration.
    This is a core pre-flight check for any job.
    """
    for conn in connections:
        if conn is None:
            continue
        
        conn_logger = logging.getLogger(conn.__class__.__name__)
        conn_logger.info(f"Validating {conn.type_} connection at {conn.get_address()}")
        conn.connection_validator()()
        conn_logger.info(f"Connected to {conn.type_} at {conn.get_address()} successfully")

def engine_adapter_factory(
    executor_config: ConnectionConfig,
    external_db_config: t.Optional[ConnectionConfig] = None
) -> EngineAdapter:
    if external_db_config is not None:
        if executor_config and executor_config.type_ == "spark":
            if external_db_config.type_ == "kafka":
                from engines.spark.spark_kafka import connection

                external_db_config: KafkaConnectionConfig = external_db_config
                conn_with_kwargs = partial(
                    connection,
                    **{
                        **executor_config._static_connection_kwargs,
                        **{
                            "bootstrap_servers": external_db_config.bootstrap_servers,
                            "group_id": external_db_config.group_id,
                            "starting_mode": external_db_config.starting_mode,
                            "kafka_config": external_db_config.config,
                        },
                        **{k: v for k, v in executor_config.dict().items() if k in executor_config._connection_kwargs_keys},
                    },
                )
                engine_adapter = SparkKafkaEngineAdapter(
                    conn_with_kwargs,
                    dialect=external_db_config.type_
                )
            else:
                from engines.spark.spark_jdbc import connection, SparkJDBCCursor

                external_engine_adapter = external_db_config.create_engine_adapter()
                if external_db_config.type_ in SparkJDBCCursor.SUPPORTED_SOURCES:
                    conn_with_kwargs = partial(
                        connection,
                        **{
                            **executor_config._static_connection_kwargs,
                            **{"connection_config": external_db_config},
                            **{k: v for k, v in executor_config.dict().items() if k in executor_config._connection_kwargs_keys},
                        },
                    )
                    engine_adapter = SparkJDBCEngineAdapter(
                        conn_with_kwargs,
                        dialect=external_engine_adapter.dialect
                    )
                    engine_adapter.SCHEMA_DIFFER = external_engine_adapter.SCHEMA_DIFFER
                else:
                    engine_adapter = external_engine_adapter

            return engine_adapter
        
        return external_db_config.create_engine_adapter()

    if executor_config is not None:
        return executor_config.create_engine_adapter()
    else:
        raise ConfigError("Cannot create an engine adapter: No executor or external connection configuration was provided.")