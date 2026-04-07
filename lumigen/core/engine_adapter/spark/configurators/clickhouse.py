from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import SparkConnectionConfig, ClickHouseConnectionConfig

from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkClickHouseEngineAdapter
from engines.spark.spark_session import connection


class SparkClickHouseConfigurator(SparkAdapterConfigurator):
    """Configures Spark to connect to ClickHouse."""

    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: ClickHouseConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ != "clickhouse":
            return None

        catalog = f"{external_db_config.type_}{id(external_db_config)}"
        interface = "https" if external_db_config.port in {8443, 443} else "http"
        
        spark: SparkSession = spark_config._static_connection_kwargs.get("spark")
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
                **spark_config._static_connection_kwargs,
                **{k: v for k, v in spark_config.dict().items() if k in spark_config._connection_kwargs_keys},
                "catalog": catalog,
            },
        )
        return SparkClickHouseEngineAdapter(conn_with_kwargs, delegate_adapter=delegate_adapter)