from __future__ import annotations

import abc
import logging
import os
import sys
import typing as t
from functools import partial
from pydantic import Field, field_validator
from sqlglot.helper import subclasses
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.engine_adapter.shared import CatalogSupport
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.pydantic import PYDANTIC_MAJOR_VERSION

from utils.errors import ConfigError
from core import engine_adapter

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


logger = logging.getLogger(__name__)

# NOTE: ONLY INCLUDE ENGINES THAT SUPPORT TRANSACTION
SUPPORTED_AUDIT_ENGINES = {"oracle", "postgres", "mssql", "mysql"}


class ConnectionConfig(abc.ABC, BaseConfig):
    type_: str
    concurrent_tasks: int
    register_comments: bool
    pre_ping: bool
    pretty_sql: bool = False

    # Whether to share a  single connection across threads or create a new connection per thread.
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

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: bool = True

    type_: Literal["mssql"] = Field(alias="type", default="mssql")

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        return {
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

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return engine_adapter.MSSQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        import pymssql

        return pymssql.connect
    
    @property
    def _extra_engine_config(self) -> t.Dict[str, t.Any]:
        return {"catalog_support": CatalogSupport.REQUIRES_SET_CATALOG}
    

class KafkaConnectionConfig(ConnectionConfig):
    bootstrap_servers: str
    group_id: t.Optional[str] = None
    starting_mode: t.Optional[str] = "earliest"
    config: t.Optional[t.Dict[str, t.Any]] = {}

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False

    type_: Literal["kafka"] = Field(alias="type", default="kafka")

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
    config: t.Optional[t.Dict[str, t.Any]] = {}
    catalog: t.Optional[str] = None
    hive_support: t.Optional[bool] = True

    concurrent_tasks: int = 4
    register_comments: bool = False
    pre_ping: Literal[False] = False

    type_: Literal["spark"] = Field(alias="type", default="spark")

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

        spark_config = SparkConf()
        if self.config:
            for k, v in self.config.items():
                spark_config.set(k, v)

        if self.config_dir:
            os.environ["SPARK_CONF_DIR"] = self.config_dir
        return {
            "app_name": self.app_name,
            "conf": spark_config,
            "hive_support": self.hive_support,
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
    v = {key.replace('.', '_'): value for key, value in v.items()}

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