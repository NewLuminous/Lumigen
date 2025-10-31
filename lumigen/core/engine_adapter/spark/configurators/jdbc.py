from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, SparkConnectionConfig

from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkJDBCEngineAdapter
from engines.spark.spark_jdbc import connection, SparkJDBCCursor


class SparkJDBCConfigurator(SparkAdapterConfigurator):
    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: ConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ not in SparkJDBCCursor.SUPPORTED_SOURCES:
            return None

        # Create the standard adapter that we will delegate certain tasks to.
        delegate_adapter = external_db_config.create_engine_adapter()
        conn_with_kwargs = partial(
            connection,
            **{
                **spark_config._static_connection_kwargs,
                "dialect": delegate_adapter.dialect,
                "connection_config": external_db_config,
                **{k: v for k, v in spark_config.dict().items() if k in spark_config._connection_kwargs_keys},
            },
        )
        return SparkJDBCEngineAdapter(conn_with_kwargs, delegate_adapter=delegate_adapter)