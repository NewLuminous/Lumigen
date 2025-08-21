from __future__ import annotations
import typing as t
from sqlglot import exp, parse_one

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from core.config.connection import ConnectionConfig

from engines.spark.errors import NotSupportedError
from engines.spark.cursors import SparkStreamToBatchCursor
from engines.spark.spark_session import SparkSessionConnection


class SparkJDBCCursor(SparkStreamToBatchCursor):
    """
    This class translates the abstract `execute` command into specific calls to
    Spark's native DataFrame reader and writer for the JDBC format.
    """
    SUPPORTED_SOURCES: set = {
        "mssql", "mysql", "oracle", "postgres"
    }

    def __init__(self, spark: SparkSession, dialect: str, connection_config: "ConnectionConfig"):
        super().__init__(spark)
        self.dialect = dialect
        self.jdbc_options = self._jdbc_options_factory(connection_config)

    @classmethod
    def _jdbc_options_factory(cls, connection_config: "ConnectionConfig"):
        c = connection_config
        if c.type_ == "oracle":
            connect_string = f"jdbc:oracle:thin:@{c.host}:{c.port}/{c.service_name or c.sid}"
            jdbc_driver = "oracle.jdbc.driver.OracleDriver"
        elif c.type_ == "postgres":
            connect_string = f"jdbc:postgresql://{c.host}:{c.port}/{c.database}"
            jdbc_driver = "org.postgresql.Driver"
        elif c.type_ == "mysql":
            connect_string = f"jdbc:mysql://{c.host}:{c.port}"
            jdbc_driver = "com.mysql.cj.jdbc.Driver"
        elif c.type_ == "mssql":
            connect_string = f"jdbc:sqlserver://{c.host}:{c.port};databaseName={c.database};encrypt=true;trustServerCertificate=true"
            jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        else:
            raise NotSupportedError(f"JDBC for {c.type_} is not supported")
            
        jdbc_options = {
            "driver": jdbc_driver,
            "url": connect_string,
            "user": c.user,
            "password": c.password,
        }
        return jdbc_options

    def execute(self, query: str, df: t.Optional[DataFrame] = None, batch_id: int = 0, **kwargs) -> None:
        """
        Overrides the base `execute` method to provide JDBC-specific read/write logic.
        This method is the main entry point for this cursor's functionality.

        Args:
            query: A SQL query string.
            df: The DataFrame to write. This is None for read operations.
            batch_id: The ID of the micro-batch, provided by `foreachBatch`.
        """
        # If a streaming DataFrame is passed, delegate to the base class's `foreachBatch` setup.
        # This will then call this same `execute` method again for each micro-batch.
        if df and df.isStreaming:
            super().execute(query, df, batch_id=batch_id, **kwargs)

        parsed_exp = parse_one(query, dialect=self.dialect)
        if df and isinstance(parsed_exp, exp.Insert):
            self._write(parsed_exp, df, **kwargs)
        else:
            self._last_df = self._spark.read.format("jdbc") \
                .options(**self.jdbc_options) \
                .option("query", query) \
                .load()
            self._last_output = None
            self._last_output_cursor = 0

    def _write(self, insert_exp: exp.Insert, df: DataFrame, **kwargs):
        target_table = t.cast("exp.Expression", insert_exp.this).find(exp.Table).sql(dialect=self.dialect)
        if insert_exp.args["overwrite"]:
            # TODO: When creating table if not exists, create CLOB columns when data length > 255
            df.write.format("jdbc") \
                .options(**self.jdbc_options) \
                .option("dbtable", target_table) \
                .option("truncate", kwargs.get("truncate", True)) \
                .mode("overwrite").save()
        else:
            df.write.format("jdbc") \
                .options(**self.jdbc_options) \
                .option("dbtable", target_table) \
                .mode("append").save()


class SparkJDBCConnection(SparkSessionConnection):
    def __init__(self, spark: SparkSession, dialect: str, connection_config: "ConnectionConfig", catalog: t.Optional[str] = None):
        super().__init__(spark, catalog)
        self.dialect = dialect
        self.connection_config = connection_config

    def cursor(self) -> SparkJDBCCursor:
        super().cursor()
        return SparkJDBCCursor(self.spark, self.dialect, self.connection_config)


def connection(spark: SparkSession, dialect: str, connection_config: "ConnectionConfig", catalog: t.Optional[str] = None) -> SparkJDBCConnection:
    return SparkJDBCConnection(spark, dialect, connection_config, catalog)