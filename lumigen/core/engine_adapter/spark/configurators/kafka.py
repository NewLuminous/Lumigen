from __future__ import annotations
import typing as t
from functools import partial

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter.base import EngineAdapter
    from core.config.connection import SparkConnectionConfig, KafkaConnectionConfig

from core.engine_adapter.spark.configurator import SparkAdapterConfigurator
from core.engine_adapter import SparkKafkaEngineAdapter
from engines.spark.spark_kafka import connection


class SparkKafkaConfigurator(SparkAdapterConfigurator):
    def configure(
        self,
        spark_config: SparkConnectionConfig,
        external_db_config: KafkaConnectionConfig,
    ) -> t.Optional[EngineAdapter]:
        if external_db_config.type_ != "kafka":
            return None

        conn_with_kwargs = partial(
            connection,
            **{
                **spark_config._static_connection_kwargs,
                "bootstrap_servers": external_db_config.bootstrap_servers,
                "group_id": external_db_config.group_id,
                "starting_mode": external_db_config.starting_mode,
                "kafka_config": external_db_config.config,
                **{k: v for k, v in spark_config.dict().items() if k in spark_config._connection_kwargs_keys},
            },
        )
        return SparkKafkaEngineAdapter(conn_with_kwargs, dialect=external_db_config.type_)