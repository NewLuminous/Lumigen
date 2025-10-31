from __future__ import annotations
import typing as t
import logging
import json

if t.TYPE_CHECKING:
    from core._typing import DataFrameType
    from engines.spark.spark_kafka import SparkKafkaCursor

from core.engine_adapter.spark.base import SparkEngineAdapter
from core.engine_adapter.kafka import KafkaEngineAdapter
from utils.collections import FallbackSet
from utils.errors import UnsupportedOperationError, MissingColumnError

logger = logging.getLogger(__name__)


class SparkKafkaEngineAdapter(SparkEngineAdapter, KafkaEngineAdapter):
    SUPPORTS_REPLACE_TABLE = True

    def replace_query(
        self,
        topics: str, # List of topics separated by comma
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        topic_list = [tn.strip() for tn in topics.split(',')]
        if len(topic_list) > 1:
            raise UnsupportedOperationError(
                f"The 'REPLACE' operation for Kafka is ambiguous for multiple topics. "
                f"It is not clear which of the {len(topic_list)} topics ('{topics}') should be replaced. "
                f"Please specify a single topic for this operation."
            )
        if not topic_list:
            raise ValueError("No topic provided for 'replace_query' operation.")

        self.execute(json.dumps({"clear": {"topics": topic_list}}))
        self.insert_append(topics, df, **kwargs)
            
    def insert_append(
        self,
        topic: str,
        df: DataFrameType,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        if ',' in topic:
            raise ValueError(f"The 'APPEND' operation for Kafka only supports a single topic per call, but got '{topic}'. ")

        df_cols = FallbackSet(c.name for c in df.schema)
        norm_cols = []
        if "key" in df_cols:
            norm_cols.append(f"CAST({df_cols.get('key')} AS STRING) AS key")
        if "value" in df_cols:
            norm_cols.append(f"CAST({df_cols.get('value')} AS STRING) AS value")
        else:
            raise MissingColumnError("Required attribute 'value' not found.")
        if "headers" in df_cols:
            norm_cols.append(f"{df_cols.get('headers')} AS headers")
        if "topic" in df_cols:
            pass # The "topic" configuration option overrides the topic column anyway.
        if "partition" in df_cols:
            pass # Let the partition be calculated by the Kafka producer
        df = df.selectExpr(*norm_cols)
        
        with self.transaction():
            request = json.dumps({"produce": {"topic": topic}})
            self.execute(request, payload=df)
            
    def merge(self, *args, **kwargs) -> None:
        raise UnsupportedOperationError("Unsupported MERGE operation for Kafka sinks.")

    def table_exists(self, topics: str) -> bool:
        topic_list = [tn.strip() for tn in topics.split(',')]
        cursor: SparkKafkaCursor = self.cursor
        topic_names = cursor.filter_topics(topic_list)
        return len(topic_names) > 0