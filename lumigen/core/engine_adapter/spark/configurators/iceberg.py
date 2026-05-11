from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import SparkConnectionConfig, IcebergConnectionConfig

from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkEngineAdapter
from engines.spark.spark_session import connection


class SparkIcebergConfigurator(SparkAdapterConfigurator):
    """Configures Spark to connect to Iceberg."""

    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: IcebergConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ != "iceberg":
            return None

        catalog = f"{external_db_config.type_}{id(external_db_config)}"
        
        spark: SparkSession = spark_config._static_connection_kwargs.get("spark")
        spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        for prop in (external_db_config.catalog_properties or {}):
            if prop in {"catalog-impl", "io-impl", "metrics-reporter-impl"}:
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", external_db_config.catalog_properties[prop])
            elif prop.startswith("table-default.") or prop.startswith("table-override.") or prop.startswith("view-default.") or prop.startswith("view-override."):
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", external_db_config.catalog_properties[prop])
            elif prop.startswith("iceberg."):
                spark.conf.set(f"spark.sql.{prop}", external_db_config.catalog_properties[prop])
            elif prop in {"wap.id", "wap.branch"}:
                spark.conf.set(f"spark.{prop}", external_db_config.catalog_properties[prop])
            else:
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", external_db_config.catalog_properties[prop])

        # Explicit arguments take precedence
        spark.conf.set(f"spark.sql.catalog.{catalog}.type", external_db_config.catalog_type)
        spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", external_db_config.warehouse)
        spark.conf.set(f"spark.sql.catalog.{catalog}.uri", external_db_config.uri)
        for prop in (external_db_config.hadoop_config or {}):
            spark.conf.set(f"spark.sql.catalog.{catalog}.hadoop.{prop}", external_db_config.hadoop_config[prop])

        # Create the standard adapter that we will delegate certain tasks to.
        conn_with_kwargs = partial(
            connection,
            **{
                **spark_config._static_connection_kwargs,
                **{k: v for k, v in spark_config.dict().items() if k in spark_config._connection_kwargs_keys},
                "catalog": catalog,
            },
        )
        return SparkEngineAdapter(conn_with_kwargs)