from __future__ import annotations
import typing as t
import logging
from sqlglot import exp

from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
)

if t.TYPE_CHECKING:
    from core._typing import DataFrameType

from utils.errors import EngineAdapterError
from core.engine_adapter.spark.base import SparkEngineAdapter
from core.engine_adapter.mixins import FetchStreamMixin

logger = logging.getLogger(__name__)


@set_catalog()
class SparkKafkaEngineAdapter(SparkEngineAdapter, FetchStreamMixin):
    SUPPORTS_REPLACE_TABLE = True

    def replace_query(
        self,
        topic: str,
        df: DataFrameType,
        **kwargs: t.Any,
    ) -> None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({
            'bootstrap.servers': self.cursor.kafka_options["kafka.bootstrap.servers"]
        })
        delete_response = admin.delete_topics([topic])
        for topic, future in delete_response.items():
            try:
                future.result()  # Block until the operation is complete
                logger.info(f"Topic '{topic}' successfully deleted.")

                self.insert_append(topic, df, **kwargs)
            except Exception as e:
                logger.error(f"Failed to delete topic '{topic}'.")
                raise e
            
    def insert_append(
        self,
        topic: str,
        df: DataFrameType,
        **kwargs: t.Any,
    ) -> None:
        if df.isStreaming:
            df.writeStream.format("kafka") \
                .option("kafka.bootstrap.servers", self.cursor.kafka_options["kafka.bootstrap.servers"]) \
                .option("topic", topic) \
                .option("checkpointLocation", f"/spark-ckpts/{self.cursor.kafka_options.get('kafka.group.id', topic)}") \
                .outputMode("append").start().awaitTermination()
        else:
            df.write.format("kafka") \
                .option("kafka.bootstrap.servers", self.cursor.kafka_options["kafka.bootstrap.servers"]) \
                .option("topic", topic) \
                .mode("append").save()
            
    def merge(self, *args, **kwargs) -> None:
        raise EngineAdapterError("Unsupported MERGE operation for Kafka sinks.")
    
    def columns(
        self, topics: str, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        return self.cursor.MESSAGE_SCHEMA

    def table_exists(self, topics: str) -> bool:
        return None # TODO