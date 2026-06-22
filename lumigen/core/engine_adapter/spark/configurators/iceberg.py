from __future__ import annotations
import typing as t
import logging
from functools import partial

if t.TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from sqlmesh.core.engine_adapter.base import EngineAdapter

from core.config.connection import SparkConnectionConfig, IcebergConnectionConfig, IcebergCatalogType
from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkEngineAdapter
from engines.spark.spark_session import connection
from utils.connections import HadoopS3PropertiesSyncer, JdbcUriTranslator
from utils.errors import ConfigError

logger = logging.getLogger(__name__)


class SparkIcebergConfigurator(SparkAdapterConfigurator):
    """Configures Spark to connect to Iceberg."""

    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: IcebergConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ != "iceberg":
            return None

        spark: SparkSession = spark_config._static_connection_kwargs.get("spark")
        catalog_properties = (external_db_config.catalog_properties or {}).copy()
        hadoop_config = (external_db_config.hadoop_config or {}).copy()
        HadoopS3PropertiesSyncer.sync_inplace(hadoop_config, catalog_properties)
        
        catalog = f"{external_db_config.type_}{id(external_db_config)}"
        is_jdbc_catalog = (
            external_db_config.catalog_type in (IcebergCatalogType.JDBC.value, IcebergCatalogType.SQL.value)
            or str(catalog_properties.get("catalog-impl", "")).endswith("JdbcCatalog")
        )
        if is_jdbc_catalog:
            catalog = "default"
            if spark.conf.get(f"spark.sql.catalog.{catalog}", None):
                logger.warning(
                    f"The '{catalog}' catalog is already configured in the Spark session. "
                    f"This usually occurs when both the source and sink use JDBC catalogs. "
                    f"Because PySpark maps Python JDBC catalogs to the '{catalog}' namespace, "
                    f"the sink's configuration will overwrite the source's configuration. "
                    f"If your source and sink point to different locations or require different properties, "
                    f"this will cause read or write failures. "
                    f"Suggestion: Change the 'catalog_type' (e.g., to 'hadoop', 'rest', etc) "
                    f"for either the source or the sink."
                )
        
        spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        for prop in catalog_properties:
            if prop in {"catalog-impl", "io-impl", "metrics-reporter-impl"}:
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", catalog_properties[prop])
            elif prop.startswith("table-default.") or prop.startswith("table-override.") or prop.startswith("view-default.") or prop.startswith("view-override."):
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", catalog_properties[prop])
            elif prop.startswith("iceberg."):
                spark.conf.set(f"spark.sql.{prop}", catalog_properties[prop])
            elif prop in {"wap.id", "wap.branch"}:
                spark.conf.set(f"spark.{prop}", catalog_properties[prop])
            elif external_db_config.catalog_type == IcebergCatalogType.DYNAMODB.value and prop == "table-name":
                spark.conf.set(f"spark.sql.catalog.{catalog}.dynamodb.{prop}", catalog_properties[prop])
            elif prop == "gcp_project":
                spark.conf.set(f"spark.sql.catalog.{catalog}.gcp.bigquery.project-id", catalog_properties[prop])
            elif prop == "gcp_location":
                spark.conf.set(f"spark.sql.catalog.{catalog}.gcp.bigquery.location", catalog_properties[prop])
            else:
                spark.conf.set(f"spark.sql.catalog.{catalog}.{prop}", catalog_properties[prop])


        # Explicit arguments take precedence
        if external_db_config.catalog_type and "catalog-impl" not in catalog_properties:
            if is_jdbc_catalog:
                spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            elif external_db_config.catalog_type == IcebergCatalogType.NESSIE.value:
                spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            elif external_db_config.catalog_type == IcebergCatalogType.GLUE.value:
                spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            elif external_db_config.catalog_type == IcebergCatalogType.DYNAMODB.value:
                spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog")
            elif external_db_config.catalog_type == IcebergCatalogType.BIGQUERY.value:
                spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog")
            else:
                spark.conf.set(f"spark.sql.catalog.{catalog}.type", external_db_config.catalog_type)
        if external_db_config.warehouse:
            spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", external_db_config.warehouse)
        if external_db_config.uri:
            spark.conf.set(f"spark.sql.catalog.{catalog}.uri", external_db_config.uri)
        for prop in hadoop_config:
            spark.conf.set(f"spark.sql.catalog.{catalog}.hadoop.{prop}", hadoop_config[prop])
            
            existing_common_val = spark.conf.get(f"spark.hadoop.{prop}", None)
            new_common_val = str(hadoop_config[prop])
            if existing_common_val is None:
                spark.conf.set(f"spark.hadoop.{prop}", new_common_val)
            elif external_db_config.catalog_type == IcebergCatalogType.HIVE.value and str(existing_common_val) != new_common_val:
                raise ConfigError(
                    f"Hadoop configuration conflict detected for property '{prop}'. "
                    f"'{prop}' is already set to '{existing_common_val}' "
                    f"while the current connection is attempting to set it to '{new_common_val}'.\n"
                    f"When using multiple Hive catalogs (e.g., for both source and sink), "
                    f"they must share the exact same 'hadoop_config'.\n"
                    f"Suggestion: Align the 'hadoop_config' across your connections, "
                    f"or change one of the catalogs to a type that supports isolated configurations (e.g., 'hadoop', 'rest', etc)."
                )


        # Inferred configurations
        catalog_uri = spark.conf.get(f"spark.sql.catalog.{catalog}.uri", None)
        if external_db_config.catalog_type == IcebergCatalogType.SQL.value:
            spark.conf.set(f"spark.sql.catalog.{catalog}.uri", JdbcUriTranslator.translate_to_jdbc(
                catalog_uri if catalog_uri else "",
                user=catalog_properties.get(f"jdbc.user"),
                password=catalog_properties.get(f"jdbc.password"),
            ))

        if external_db_config.catalog_type == IcebergCatalogType.GLUE.value:
            self._set_conf_default(spark, f"spark.sql.catalog.{catalog}.glue.endpoint", catalog_uri)

        is_aws_catalog = (
            external_db_config.catalog_type in (IcebergCatalogType.GLUE.value, IcebergCatalogType.DYNAMODB.value)
            or spark.conf.get(f"spark.sql.catalog.{catalog}.catalog-impl", "").startswith("org.apache.iceberg.aws.")
        )
        if is_aws_catalog:
            key = catalog_properties.get("s3.access-key-id")
            secret = catalog_properties.get("s3.secret-access-key")
            if (not key) or (not secret):
                # Use HadoopFileIO instead of S3FileIO for unauthenticated access to S3 warehouse
                # since AWS services do not accept software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
                spark.conf.set(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

        if str(spark.conf.get(f"spark.sql.catalog.{catalog}.io-impl", "")).endswith("HadoopFileIO") \
        or external_db_config.catalog_type in {
            IcebergCatalogType.HADOOP.value, IcebergCatalogType.NESSIE.value,
        } or is_jdbc_catalog:
            key = spark.conf.get(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.access.key", None) or spark.conf.get(f"spark.hadoop.fs.s3a.access.key", None)
            secret = spark.conf.get(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.secret.key", None) or spark.conf.get(f"spark.hadoop.fs.s3a.secret.key", None)
            if (not key) or (not secret):
                spark.conf.set(f"spark.sql.catalog.{catalog}.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")


        if str(spark.conf.get(f"spark.sql.catalog.{catalog}.io-impl", "")).endswith("S3FileIO") \
        or external_db_config.catalog_type == IcebergCatalogType.REST.value:
            key = catalog_properties.get("s3.access-key-id") or catalog_properties.get("client.access-key-id")
            secret = catalog_properties.get("s3.secret-access-key") or catalog_properties.get("client.secret-access-key")
            if (not key) or (not secret):
                spark.conf.set(f"spark.sql.catalog.{catalog}.client.credentials-provider", "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider")
            if key and secret:
                self._set_conf_default(spark, f"spark.sql.catalog.{catalog}.credential", f"{key}:{secret}")

        if is_aws_catalog:
            self._set_conf_default(spark, f"spark.sql.catalog.{catalog}.client.access-key-id", "any")
            self._set_conf_default(spark, f"spark.sql.catalog.{catalog}.client.secret-access-key", "any")

        if external_db_config.catalog_type == IcebergCatalogType.REST.value:
            self._set_conf_default(spark, f"spark.sql.catalog.{catalog}.token-exchange-enabled", "false")


        # System properties
        sc = spark.sparkContext
        sc._jvm.java.lang.System.setProperty("aws.java.v1.disableDeprecationAnnouncement", "true")
        if is_aws_catalog:
            sc._jvm.java.lang.System.setProperty("aws.accessKeyId", spark.conf.get(f"spark.sql.catalog.{catalog}.client.access-key-id", None))
            sc._jvm.java.lang.System.setProperty("aws.secretAccessKey", spark.conf.get(f"spark.sql.catalog.{catalog}.client.secret-access-key", None))
            sc._jvm.java.lang.System.setProperty("aws.region", spark.conf.get(f"spark.sql.catalog.{catalog}.client.region", None))


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