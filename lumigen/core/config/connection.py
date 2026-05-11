from __future__ import annotations

import abc
import logging
import os
import importlib
import sys
import typing as t
from functools import partial
from pydantic import Field, field_validator, model_validator
from pydantic_core import from_json
from sqlglot.helper import subclasses
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.engine_adapter.shared import CatalogSupport
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.pydantic import PYDANTIC_MAJOR_VERSION, get_concrete_types_from_typehint
from sqlmesh.utils import str_to_bool

from utils.errors import ConfigError
from core import engine_adapter

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


logger = logging.getLogger(__name__)

# NOTE: ONLY INCLUDE ENGINES THAT SUPPORT TRANSACTION
SUPPORTED_AUDIT_ENGINES = {"oracle", "postgres", "mssql", "mysql"}


def _get_engine_import_validator(
    import_name: str, engine_type: str, extra_name: t.Optional[str] = None, decorate: bool = True
) -> t.Callable:
    extra_name = extra_name or engine_type

    def validate(cls: t.Any, data: t.Any) -> t.Any:
        check_import = (
            str_to_bool(str(data.pop("check_import", True))) if isinstance(data, dict) else True
        )
        if not check_import:
            return data
        try:
            importlib.import_module(import_name)
        except ImportError:
            logger.exception("Failed to import the engine library")

            raise ConfigError(
                f"Failed to import the '{engine_type}' engine library. This may be due to a missing "
                "or incompatible installation. Please ensure the required dependency is installed."
            )

        return data

    return model_validator(mode="before")(validate) if decorate else validate

class ConnectionConfig(abc.ABC, BaseConfig):
    type_: str
    concurrent_tasks: int
    register_comments: bool
    pre_ping: bool
    pretty_sql: bool = False
    schema_differ_overrides: t.Optional[t.Dict[str, t.Any]] = None
    catalog_type_overrides: t.Optional[t.Dict[str, str]] = None

    # Whether to share a single connection across threads or create a new connection per thread.
    shared_connection: t.ClassVar[bool] = False

    @property
    @abc.abstractmethod
    def _connection_kwargs_keys(self) -> t.Set[str]:
        """keywords that should be passed into the connection"""

    @property
    @abc.abstractmethod
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        """The engine adapter for this connection"""

    @property
    @abc.abstractmethod
    def _connection_factory(self) -> t.Callable:
        """A function that is called to return a connection object for the given Engine Adapter"""

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        """The static connection kwargs for this connection"""
        return {}

    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        """kwargs that are for execution config only"""
        return {}

    @property
    def _cursor_kwargs(self) -> t.Optional[t.Dict[str, t.Any]]:
        """Key-value arguments that will be passed during cursor construction."""
        return None

    @property
    def _cursor_init(self) -> t.Optional[t.Callable[[t.Any], None]]:
        """A function that is called to initialize the cursor"""
        return None
    
    @property
    def is_supported_for_audit(self) -> bool:
        """Whether this engine is officially supported for use as the audit store."""
        return self.type_ in SUPPORTED_AUDIT_ENGINES

    @property
    def _connection_factory_with_kwargs(self) -> t.Callable[[], t.Any]:
        """A function that is called to return a connection object for the given Engine Adapter"""
        return partial(
            self._connection_factory,
            **{
                **self._static_connection_kwargs,
                **{k: v for k, v in self.dict().items() if k in self._connection_kwargs_keys},
            },
        )

    def connection_validator(self) -> t.Callable[[], None]:
        """A function that validates the connection configuration"""
        return self.create_engine_adapter().ping

    def create_engine_adapter(
        self, register_comments_override: bool = False, concurrent_tasks: t.Optional[int] = None
    ) -> EngineAdapter:
        """Returns a new instance of the Engine Adapter."""
        concurrent_tasks = concurrent_tasks or self.concurrent_tasks
        return self._engine_adapter(
            self._connection_factory_with_kwargs,
            multithreaded=concurrent_tasks > 1,
            cursor_kwargs=self._cursor_kwargs,
            default_catalog=self.get_catalog(),
            cursor_init=self._cursor_init,
            register_comments=register_comments_override or self.register_comments,
            pre_ping=self.pre_ping,
            pretty_sql=self.pretty_sql,
            shared_connection=self.shared_connection,
            schema_differ_overrides=self.schema_differ_overrides,
            catalog_type_overrides=self.catalog_type_overrides,
            **self._extra_engine_config,
        )

    def get_catalog(self) -> t.Optional[str]:
        """The catalog for this connection"""
        if hasattr(self, "catalog"):
            return self.catalog
        if hasattr(self, "database"):
            return self.database
        if hasattr(self, "db"):
            return self.db
        return None
    
    @model_validator(mode="before")
    @classmethod
    def _expand_json_strings_to_concrete_types(cls, data: t.Any) -> t.Any:
        if data and isinstance(data, dict):
            for maybe_json_field_name in cls._get_list_and_dict_field_names():
                if (value := data.get(maybe_json_field_name)) and isinstance(value, str):
                    # crude JSON check as we dont want to try and parse every string we get
                    value = value.strip()
                    if value.startswith("{") or value.startswith("["):
                        data[maybe_json_field_name] = from_json(value)

        return data

    @classmethod
    def _get_list_and_dict_field_names(cls) -> t.Set[str]:
        field_names = set()
        for name, field in cls.model_fields.items():
            if field.annotation:
                field_types = get_concrete_types_from_typehint(field.annotation)

                # check if the field type is something that could concievably be supplied as a json string
                if any(ft is t for t in (list, tuple, set, dict) for ft in field_types):
                    field_names.add(name)

        return field_names
    
    def get_address(self) -> t.Optional[str]:
        """The address for this connection"""
        addr_str = ""
        if hasattr(self, "bootstrap_servers"):
            addr_str += self.bootstrap_servers
        if hasattr(self, "host"):
            addr_str += self.host
        if hasattr(self, "port"):
            addr_str += f":{self.port}"
        if hasattr(self, "service_name"):
            addr_str += f"/{self.service_name}"
        if self.get_catalog():
            addr_str += f"/{self.get_catalog()}"
        return addr_str if addr_str else None


class ClickHouseConnectionConfig(ConnectionConfig):
    """
    ClickHouse Connection Configuration.

    Property reference: https://clickhouse.com/docs/en/integrations/python#client-initialization
    """

    host: str
    username: str
    password: t.Optional[str] = None
    port: t.Optional[int] = None
    database: t.Optional[str] = None
    cluster: t.Optional[str] = None
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    query_limit: int = 0
    use_compression: bool = True
    compression_method: t.Optional[str] = None
    connection_settings: t.Optional[t.Dict[str, t.Any]] = None
    http_proxy: t.Optional[str] = None
    # HTTPS/TLS settings
    verify: bool = True
    ca_cert: t.Optional[str] = None
    client_cert: t.Optional[str] = None
    client_cert_key: t.Optional[str] = None
    https_proxy: t.Optional[str] = None
    server_host_name: t.Optional[str] = None
    tls_mode: t.Optional[str] = None

    concurrent_tasks: int = 1
    register_comments: bool = False
    pre_ping: bool = False

    # This object expects options from urllib3 and also from clickhouse-connect
    # See:
    # * https://urllib3.readthedocs.io/en/stable/advanced-usage.html
    # * https://clickhouse.com/docs/en/integrations/python#customizing-the-http-connection-pool
    connection_pool_options: t.Optional[t.Dict[str, t.Any]] = None

    type_: t.Literal["clickhouse"] = Field(alias="type", default="clickhouse")

    _engine_import_validator = _get_engine_import_validator("clickhouse_connect", "clickhouse")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        kwargs = {
            "host",
            "username",
            "port",
            "password",
            "database",
            "connect_timeout",
            "send_receive_timeout",
            "query_limit",
            "http_proxy",
            "verify",
            "ca_cert",
            "client_cert",
            "client_cert_key",
            "https_proxy",
            "server_host_name",
            "tls_mode",
        }
        return kwargs

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.ClickHouseEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from clickhouse_connect.dbapi import connect  # type: ignore
        from clickhouse_connect.driver import httputil  # type: ignore
        from functools import partial

        pool_manager_options: t.Dict[str, t.Any] = dict(
            # Match the maxsize to the number of concurrent tasks
            maxsize=self.concurrent_tasks,
            # Block if there are no free connections
            block=True,
            verify=self.verify,
            ca_cert=self.ca_cert,
            client_cert=self.client_cert,
            client_cert_key=self.client_cert_key,
            https_proxy=self.https_proxy,
        )
        # this doesn't happen automatically because we always supply our own pool manager to the connection
        # https://github.com/ClickHouse/clickhouse-connect/blob/3a7f4b04cad29c7c2536661b831fb744248e2ec0/clickhouse_connect/driver/httpclient.py#L109
        if self.server_host_name:
            pool_manager_options["server_hostname"] = self.server_host_name
            if self.verify:
                pool_manager_options["assert_hostname"] = self.server_host_name
        if self.connection_pool_options:
            pool_manager_options.update(self.connection_pool_options)
        pool_mgr = httputil.get_pool_manager(**pool_manager_options)

        return partial(connect, pool_mgr=pool_mgr)

    @property
    def cloud_mode(self) -> bool:
        return "clickhouse.cloud" in self.host

    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        return {"cluster": self.cluster, "cloud_mode": self.cloud_mode}

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        # False = no compression
        # True = Clickhouse default compression method
        # string = specific compression method
        compress: bool | str = self.use_compression
        if compress and self.compression_method:
            compress = self.compression_method

        # Clickhouse system settings passed to connection
        # https://clickhouse.com/docs/en/operations/settings/settings
        # - below are set to align with dbt-clickhouse
        # - https://github.com/ClickHouse/dbt-clickhouse/blob/44d26308ea6a3c8ead25c280164aa88191f05f47/dbt/adapters/clickhouse/dbclient.py#L77
        settings = self.connection_settings or {}
        #  mutations_sync = 2: "The query waits for all mutations [ALTER statements] to complete on all replicas (if they exist)"
        settings["mutations_sync"] = "2"
        #  insert_distributed_sync = 1: "INSERT operation succeeds only after all the data is saved on all shards"
        settings["insert_distributed_sync"] = "1"
        if self.cluster or self.cloud_mode:
            # database_replicated_enforce_synchronous_settings = 1:
            #   - "Enforces synchronous waiting for some queries"
            #   - https://github.com/ClickHouse/ClickHouse/blob/ccaa8d03a9351efc16625340268b9caffa8a22ba/src/Core/Settings.h#L709
            settings["database_replicated_enforce_synchronous_settings"] = "1"
            # insert_quorum = auto:
            #   - "INSERT succeeds only when ClickHouse manages to correctly write data to the insert_quorum of replicas during
            #       the insert_quorum_timeout"
            #   - "use majority number (number_of_replicas / 2 + 1) as quorum number"
            settings["insert_quorum"] = "auto"

        return {
            "compress": compress,
            "client_name": f"Lumigen",
            **settings,
        }


class OracleConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    port: int
    service_name: t.Optional[str] = None
    sid: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["oracle"] = Field(alias="type", default="oracle")

    _engine_import_validator = _get_engine_import_validator("oracledb", "oracle")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "host",
            "user",
            "password",
            "port",
            "service_name",
            "sid",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.OracleEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from oracledb import connect

        return connect


class PostgresConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    port: int
    database: str
    keepalives_idle: t.Optional[int] = None
    connect_timeout: int = 10
    role: t.Optional[str] = None
    sslmode: t.Optional[str] = None
    application_name: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["postgres"] = Field(alias="type", default="postgres")

    _engine_import_validator = _get_engine_import_validator("psycopg2", "postgres")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "host",
            "user",
            "password",
            "port",
            "database",
            "keepalives_idle",
            "connect_timeout",
            "sslmode",
            "application_name",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.PostgresEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from psycopg2 import connect

        return connect

    @property
    def _cursor_init(self) -> t.Optional[t.Callable[[t.Any], None]]:
        if not self.role:
            return None

        def init(cursor: t.Any) -> None:
            cursor.execute(f"SET ROLE {self.role}")

        return init


class MySQLConnectionConfig(ConnectionConfig):
    host: str
    user: str
    password: str
    port: t.Optional[int] = None
    database: t.Optional[str] = None
    charset: t.Optional[str] = None
    collation: t.Optional[str] = None
    ssl_disabled: t.Optional[bool] = None

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["mysql"] = Field(alias="type", default="mysql")

    _engine_import_validator = _get_engine_import_validator("pymysql", "mysql")

    @property
    def _cursor_kwargs(self) -> t.Optional[t.Dict[str, t.Any]]:
        """Key-value arguments that will be passed during cursor construction."""
        return {"buffered": True}

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        connection_keys = {
            "host",
            "user",
            "password",
        }
        if self.port is not None:
            connection_keys.add("port")
        if self.database is not None:
            connection_keys.add("database")
        if self.charset is not None:
            connection_keys.add("charset")
        if self.collation is not None:
            connection_keys.add("collation")
        if self.ssl_disabled is not None:
            connection_keys.add("ssl_disabled")
        return connection_keys

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MySQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from pymysql import connect

        return connect


class MSSQLConnectionConfig(ConnectionConfig):
    host: str
    user: t.Optional[str] = None
    password: t.Optional[str] = None
    database: t.Optional[str] = ""
    timeout: t.Optional[int] = 0
    login_timeout: t.Optional[int] = 60
    charset: t.Optional[str] = "UTF-8"
    appname: t.Optional[str] = None
    port: t.Optional[int] = 1433
    conn_properties: t.Optional[t.Union[t.Iterable[str], str]] = None
    autocommit: t.Optional[bool] = False
    tds_version: t.Optional[str] = None

    # Driver options
    driver: t.Literal["pymssql", "pyodbc"] = "pymssql"
    # PyODBC specific options
    driver_name: t.Optional[str] = None  # e.g. "ODBC Driver 18 for SQL Server"
    trust_server_certificate: t.Optional[bool] = None
    encrypt: t.Optional[bool] = None
    # Dictionary of arbitrary ODBC connection properties
    # See: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute
    odbc_properties: t.Optional[t.Dict[str, t.Any]] = None

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["mssql"] = Field(alias="type", default="mssql")

    @model_validator(mode="before")
    @classmethod
    def _mssql_engine_import_validator(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        driver = data.get("driver", "pymssql")

        # Define the mapping of driver to import module and extra name
        driver_configs = {"pymssql": ("pymssql", "mssql"), "pyodbc": ("pyodbc", "mssql-odbc")}

        if driver not in driver_configs:
            raise ValueError(f"Unsupported driver: {driver}")

        import_module, extra_name = driver_configs[driver]

        # Use _get_engine_import_validator with decorate=False to get the raw validation function
        # This avoids the __wrapped__ issue in Python 3.9
        validator_func = _get_engine_import_validator(
            import_module, driver, extra_name, decorate=False
        )

        # Call the raw validation function directly
        return validator_func(cls, data)

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        base_keys = {
            "host",
            "user",
            "password",
            "database",
            "timeout",
            "login_timeout",
            "charset",
            "appname",
            "port",
            "conn_properties",
            "autocommit",
            "tds_version",
        }

        if self.driver == "pyodbc":
            base_keys.update(
                {
                    "driver_name",
                    "trust_server_certificate",
                    "encrypt",
                    "odbc_properties",
                }
            )
            # Remove pymssql-specific parameters
            base_keys.discard("tds_version")
            base_keys.discard("conn_properties")

        return base_keys

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MSSQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        if self.driver == "pymssql":
            import pymssql

            return pymssql.connect

        import pyodbc

        def connect(**kwargs: t.Any) -> t.Callable:
            # Extract parameters for connection string
            host = kwargs.pop("host")
            port = kwargs.pop("port", 1433)
            database = kwargs.pop("database", "")
            user = kwargs.pop("user", None)
            password = kwargs.pop("password", None)
            driver_name = kwargs.pop("driver_name", "ODBC Driver 18 for SQL Server")
            trust_server_certificate = kwargs.pop("trust_server_certificate", False)
            encrypt = kwargs.pop("encrypt", True)
            login_timeout = kwargs.pop("login_timeout", 60)

            # Build connection string
            conn_str_parts = [
                f"DRIVER={{{driver_name}}}",
                f"SERVER={host},{port}",
            ]

            if database:
                conn_str_parts.append(f"DATABASE={database}")

            # Add security options
            conn_str_parts.append(f"Encrypt={'YES' if encrypt else 'NO'}")
            if trust_server_certificate:
                conn_str_parts.append("TrustServerCertificate=YES")

            conn_str_parts.append(f"Connection Timeout={login_timeout}")

            # Standard SQL Server authentication
            if user:
                conn_str_parts.append(f"UID={user}")
            if password:
                conn_str_parts.append(f"PWD={password}")

            # Add any additional ODBC properties from the odbc_properties dictionary
            if self.odbc_properties:
                for key, value in self.odbc_properties.items():
                    # Skip properties that we've already set above
                    if key.lower() in (
                        "driver",
                        "server",
                        "database",
                        "uid",
                        "pwd",
                        "encrypt",
                        "trustservercertificate",
                        "connection timeout",
                    ):
                        continue

                    # Handle boolean values properly
                    if isinstance(value, bool):
                        conn_str_parts.append(f"{key}={'YES' if value else 'NO'}")
                    else:
                        conn_str_parts.append(f"{key}={value}")

            # Create the connection string
            conn_str = ";".join(conn_str_parts)

            conn = pyodbc.connect(conn_str, autocommit=kwargs.get("autocommit", False))

            # Set up output converters for MSSQL-specific data types
            # Handle SQL type -155 (DATETIMEOFFSET) which is not yet supported by pyodbc
            # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
            def handle_datetimeoffset(dto_value: t.Any) -> t.Any:
                from datetime import datetime, timedelta, timezone
                import struct

                # Unpack the DATETIMEOFFSET binary format:
                # Format: <6hI2h = (year, month, day, hour, minute, second, nanoseconds, tz_hour_offset, tz_minute_offset)
                tup = struct.unpack("<6hI2h", dto_value)
                return datetime(
                    tup[0],
                    tup[1],
                    tup[2],
                    tup[3],
                    tup[4],
                    tup[5],
                    tup[6] // 1000,
                    timezone(timedelta(hours=tup[7], minutes=tup[8])),
                )

            conn.add_output_converter(-155, handle_datetimeoffset)

            return conn

        return connect
    
    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        return {"catalog_support": CatalogSupport.REQUIRES_SET_CATALOG}
    

class IcebergConnectionConfig(ConnectionConfig):
    catalog_type: Literal["hive", "hadoop", "rest", "glue", "jdbc", "nessie"]
    warehouse: t.Optional[str] = None
    uri: t.Optional[str] = None
    catalog_properties: t.Optional[t.Dict[str, t.Any]] = {}
    connect_timeout: int = 10

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["iceberg"] = Field(alias="type", default="iceberg")

    _engine_import_validator = _get_engine_import_validator("pyiceberg", "iceberg")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "catalog_type",
            "warehouse",
            "uri",
            "catalog_properties",
            "connect_timeout",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.IcebergEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from engines.iceberg.client import connect

        return connect


class KafkaConnectionConfig(ConnectionConfig):
    bootstrap_servers: str
    group_id: t.Optional[str] = None
    starting_mode: t.Optional[str] = "earliest"
    config: t.Optional[t.Dict[str, t.Any]] = {}

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False

    type_: Literal["kafka"] = Field(alias="type", default="kafka")

    _engine_import_validator = _get_engine_import_validator("confluent_kafka", "kafka")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "bootstrap_servers",
            "group_id",
            "config",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.KafkaEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from engines.kafka.kafka_client import connection

        return connection


class SparkConnectionConfig(ConnectionConfig):
    """
    Vanilla Spark Connection Configuration.
    """

    app_name: t.Optional[str] = None
    config_dir: t.Optional[str] = None
    log_level: t.Optional[str] = "INFO"
    config: t.Optional[t.Dict[str, t.Any]] = {}
    catalog: t.Optional[str] = None
    hive_support: t.Optional[bool] = True

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False

    type_: Literal["spark"] = Field(alias="type", default="spark")

    _engine_import_validator = _get_engine_import_validator("pyspark", "spark")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "catalog",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.SparkEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from engines.spark.spark_session import connection

        return connection

    @property
    def _static_connection_kwargs(self) -> t.Dict[str, t.Any]:
        from pyspark.conf import SparkConf
        from core.engine_context import SQLContextManager

        spark_config = SparkConf()
        if self.config:
            # Static configurations
            static_confs = {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", # For Iceberg
            }
            for k, v in static_confs.items():
                if self.config.get(k):
                    self.config[k] = self.config[k] + "," + v
                else:
                    self.config[k] = v

            for k, v in self.config.items():
                spark_config.set(k, v)

        if self.config_dir:
            os.environ["SPARK_CONF_DIR"] = self.config_dir
        return {
            "spark": SQLContextManager().get_spark_session(
                app_name=self.app_name,
                conf=spark_config,
                hive_support=self.hive_support,
                log_level=self.log_level,
            )
        }
    

class FakeConnectionConfig(ConnectionConfig):
    schema_type: t.List[t.Dict[str, t.Any]]
    num_row: t.Optional[int] = 5

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False
    type_: Literal["fake"] = Field(alias="type", default="fake")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "schema_type",
            "num_row",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        from core.engine_adapter.fake import FakeEngineAdapter
        return FakeEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from engines.fake import connection
        return connection
    

class ConsoleConfig(ConnectionConfig):
    print_data: t.Optional[bool] = True
    print_limit_per_batch: t.Optional[int] = 5
    print_delay: t.Optional[int] = 5

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False
    type_: Literal["console"] = Field(alias="type", default="console")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
            "print_data",
            "print_limit_per_batch",
            "print_delay",
        }

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        from core.engine_adapter.fake import FakeEngineAdapter
        return FakeEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        from engines.fake import connection
        return connection


CONNECTION_CONFIG_TO_TYPE = {
    # Map all subclasses of ConnectionConfig to the value of their `type_` field.
    tpe.all_field_infos()["type_"].default: tpe
    for tpe in subclasses(
        __name__,
        ConnectionConfig,
        exclude=(ConnectionConfig,),
    )
}


SEATUNNEL_CONNECTORS = ["Console", "Jdbc"]
def seatunnel_validator(connector: str) -> str:
    if connector not in SEATUNNEL_CONNECTORS:
        raise ConfigError(f"Connector type '{connector}' not supported by Apache SeaTunnel.")

    return connector


class SeaTunnelConnectorConfig(BaseConfig):
    type_: str = Field(alias="type", default="seatunnel")
    options: t.Dict[str, t.Any]
    
    _seatunnel_validator = field_validator(
        "type_",
        mode="before",
        check_fields=False,
    )(seatunnel_validator)


def parse_connection_config(v: t.Dict[str, t.Any]) -> ConnectionConfig | SeaTunnelConnectorConfig:
    if "type" not in v:
        raise ConfigError("Missing connection type.")

    # Normalize keys
    v = {key.replace('.', '_').replace('-', '_'): value for key, value in v.items()}

    connection_type = v["type"]
    if connection_type not in CONNECTION_CONFIG_TO_TYPE:
        if connection_type[:1].isupper():
            try:
                return SeaTunnelConnectorConfig.parse_obj({"type":connection_type, "options":{k:v[k] for k in v if k!="type"}})
            except ConfigError:
                pass
        raise ConfigError(f"Unknown connection type '{connection_type}'.") from None

    return CONNECTION_CONFIG_TO_TYPE[connection_type](**v)


def connection_config_validator(
    cls: t.Type, v: ConnectionConfig | t.Dict[str, t.Any] | None
) -> ConnectionConfig | None | SeaTunnelConnectorConfig:
    if v is None or isinstance(v, ConnectionConfig):
        return v
    return parse_connection_config(v)


if t.TYPE_CHECKING and sys.version_info >= (3, 10):
    # TypeAlias hasn't been introduced until Python 3.10 which means that we can't use it
    # outside the TYPE_CHECKING guard.
    SerializableConnectionConfig: t.TypeAlias = ConnectionConfig  # type: ignore
elif PYDANTIC_MAJOR_VERSION >= 2:
    import pydantic

    # Workaround for https://docs.pydantic.dev/latest/concepts/serialization/#serializing-with-duck-typing
    SerializableConnectionConfig = pydantic.SerializeAsAny[ConnectionConfig]  # type: ignore
else:
    SerializableConnectionConfig = ConnectionConfig  # type: ignore