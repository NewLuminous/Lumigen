from __future__ import annotations
import typing as t

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

if t.TYPE_CHECKING:
    from core.config.connection import ConnectionConfig

from core.engine_context import SQLContextManager
from engines.spark.errors import NotSupportedError
from engines.spark.spark_session import SparkSessionCursor, SparkSessionConnection


class SparkJDBCCursor(SparkSessionCursor):
    SUPPORTED_SOURCES: set = {
        "mssql", "mysql", "oracle", "postgres"
    }

    def __init__(self, spark: SparkSession, connection_config: "ConnectionConfig"):
        super().__init__(spark)
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

    def execute(self, query: str, parameters: t.Optional[t.Any] = None) -> None:
        if parameters:
            raise NotSupportedError("Parameterized queries are not supported")

        self._last_df = self._spark.read.format("jdbc") \
            .options(**self.jdbc_options) \
            .option("query", query) \
            .load()
        self._last_output = None
        self._last_output_cursor = 0


class SparkJDBCConnection(SparkSessionConnection):
    def __init__(self, spark: SparkSession, connection_config: "ConnectionConfig", catalog: t.Optional[str] = None):
        super().__init__(spark, catalog)
        self.connection_config = connection_config
        self.engine_adapter = connection_config.create_engine_adapter()

    def cursor(self) -> SparkJDBCCursor:
        super().cursor()
        return SparkJDBCCursor(self.spark, self.connection_config)


def connection(app_name: t.Optional[str], conf: SparkConf, connection_config: "ConnectionConfig",
               catalog: t.Optional[str] = None,
               hive_support: t.Optional[bool] = True,
               ) -> SparkJDBCConnection:
    manager = SQLContextManager()
    spark = manager.get_spark_session(app_name=app_name, conf=conf, hive_support=hive_support)
    return SparkJDBCConnection(spark, connection_config, catalog)