from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, KafkaConnectionConfig, ClickHouseConnectionConfig

from core.adapters import AdapterResolver


class SparkAdapterResolver(AdapterResolver):
    """Resolves specialized Spark engine adapters for various external systems."""

    def resolve(self, external_db_config: ConnectionConfig) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ == "kafka":
            from engines.spark.spark_kafka import connection
            from core.engine_adapter import SparkKafkaEngineAdapter
            
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

        elif external_db_config.type_ == "clickhouse":
            from engines.spark.spark_session import connection
            from core.engine_adapter import SparkClickHouseEngineAdapter

            external_db_config: ClickHouseConnectionConfig = external_db_config
            catalog = external_db_config.type_
            interface = "https" if external_db_config.port in {8443, 443} else "http"

            spark: SparkSession = self.executor_config._static_connection_kwargs.get("spark")
            spark.conf.set(f"spark.sql.catalog.{catalog}", "com.clickhouse.spark.ClickHouseCatalog")
            spark.conf.set(f"spark.sql.catalog.{catalog}.host", external_db_config.host)
            spark.conf.set(f"spark.sql.catalog.{catalog}.protocol", interface)
            spark.conf.set(f"spark.sql.catalog.{catalog}.http_port", external_db_config.port)
            spark.conf.set(f"spark.sql.catalog.{catalog}.user", external_db_config.username)
            spark.conf.set(f"spark.sql.catalog.{catalog}.password", external_db_config.password)
            spark.conf.set(f"spark.sql.catalog.{catalog}.database", external_db_config.database)
            if external_db_config.cloud_mode:
                spark.conf.set(f"spark.sql.catalog.{catalog}.option.ssl", True)
                spark.conf.set(f"spark.sql.catalog.{catalog}.option.ssl_mode", external_db_config.tls_mode)
            
            # Fix pyspark.sql.utils.AnalysisException:  [-1] Unsupported ClickHouse expression: FuncExpr[tuple()]
            spark.conf.set(f"spark.clickhouse.ignoreUnsupportedTransform", True)
            
            spark.conf.set(f"spark.clickhouse.read.format", "binary")
            #spark.conf.set(f"spark.clickhouse.useNullableQuerySchema", False)
            #spark.conf.set(f"spark.clickhouse.write.batchSize", 10000)
            spark.conf.set(f"spark.clickhouse.write.compression.codec", "none") # or "lz4"

            # Create the standard adapter that we will delegate certain tasks to.
            delegate_adapter = external_db_config.create_engine_adapter()
            conn_with_kwargs = partial(
                connection,
                **{
                    **self.executor_config._static_connection_kwargs,
                    **{k: v for k, v in self.executor_config.dict().items() if k in self.executor_config._connection_kwargs_keys},
                    "catalog": catalog,
                },
            )
            return SparkClickHouseEngineAdapter(conn_with_kwargs, delegate_adapter=delegate_adapter)
        else:
            from engines.spark.spark_jdbc import connection, SparkJDBCCursor
            from core.engine_adapter import SparkJDBCEngineAdapter

            if external_db_config.type_ in SparkJDBCCursor.SUPPORTED_SOURCES:
                # Create the standard adapter that we will delegate certain tasks to.
                delegate_adapter = external_db_config.create_engine_adapter()
                conn_with_kwargs = partial(
                    connection,
                    **{
                        **self.executor_config._static_connection_kwargs,
                        "dialect": delegate_adapter.dialect,
                        "connection_config": external_db_config,
                        **{k: v for k, v in self.executor_config.dict().items() if k in self.executor_config._connection_kwargs_keys},
                    },
                )
                return SparkJDBCEngineAdapter(conn_with_kwargs, delegate_adapter=delegate_adapter)
        
        return None