from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import ConnectionConfig, SparkConnectionConfig

from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkFileSystemEngineAdapter
from engines.spark.spark_fs import connection, SparkFileSystemCursor


class SparkFileSystemConfigurator(SparkAdapterConfigurator):
    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: ConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ not in SparkFileSystemCursor.SUPPORTED_FILE_SYSTEMS:
            return None

        fs_connection_factory = external_db_config._connection_factory_with_kwargs()
        spark_conn_with_kwargs = partial(
            connection,
            **{
                **spark_config._static_connection_kwargs,
                "fs_type": external_db_config.type_,
                "fs_connection_factory": fs_connection_factory,
                **{k: v for k, v in spark_config.dict().items() if k in spark_config._connection_kwargs_keys},
            },
        )
        return SparkFileSystemEngineAdapter(spark_conn_with_kwargs)