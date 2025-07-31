"""Contains KafkaEngineAdapter."""

from __future__ import annotations
import typing as t

import logging
import json
import contextlib
from collections import defaultdict
from sqlglot import exp
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)
import pandas as pd
import numpy as np
import pyspark.sql
import pyspark.sql.functions as F

if t.TYPE_CHECKING:
    from confluent_kafka import KafkaException
    from core._typing import DataFrameType
    from engines.kafka.kafka_client import KafkaClientCursor

from core.config.job import DataLoadMode
from core.engine_adapter.mixins import EngineAdapterMixin, FetchStreamMixin, QueryBuilderMixin, WatermarkStrategyMixin
from engines.kafka.kafka_client import MESSAGE_SCHEMA
from utils import FallbackSet
from utils.errors import EngineAdapterError, MissingColumnError, UnsupportedDataFrameError
from utils.parsing import extract_main_table

logger = logging.getLogger(__name__)


class KafkaEngineAdapter(FetchStreamMixin, EngineAdapterMixin, QueryBuilderMixin, WatermarkStrategyMixin):
    DIALECT: str = "kafka"

    def build_query(
        self,
        path: t.Optional[str] = None,
        query: t.Optional[str] = None,
        data_load_mode: t.Optional[DataLoadMode] = None,
        watermark_column: t.Optional[str] = None,
        last_watermark_value: t.Optional[t.Any] = None,
        **kwargs: t.Any,
    ) -> t.Any:
        if query:
            try:
                topics_str = extract_main_table(query, quoted=False)
            except:
                topics_str = query
        if path:
            topics_str = path

        topics = [tp.strip() for tp in topics_str.split(',')]
        
        op_params = {
            "topics": topics,
        }
        if data_load_mode == DataLoadMode.INCREMENTAL and last_watermark_value:
            op_params["stored_offsets"] = last_watermark_value
        return json.dumps({
            "consume": op_params
        })

    def fetch_batch_df(
        self,
        topic: t.Union[exp.Expression, str],
        batch_size: int = None,
        quote_identifiers: bool = False,
    ) -> t.Iterator[DataFrameType]:
        batches = self.fetchbatch(topic, batch_size, quote_identifiers=quote_identifiers)
        for batch in batches:
            logger.info(f"Fetched {len(batch)} Kafka messages")
            yield pd.DataFrame(batch, columns=list(MESSAGE_SCHEMA.keys()))

    @contextlib.contextmanager
    def transaction(
        self,
        condition: t.Optional[bool] = None,
    ) -> t.Iterator[None]:
        def rewind_consumer_offsets():
            pass # TODO

        if (
            self._connection_pool.is_transaction_active
            or not self.SUPPORTS_TRANSACTIONS
            or (condition is not None and not condition)
        ):
            yield
            return

        if self._pre_ping:
            try:
                logger.debug("Pinging the database to check the connection")
                self.ping()
            except Exception:
                logger.info("Connection to the database was lost. Reconnecting...")
                self._connection_pool.close()

        try:
            self._connection_pool.begin()
            yield
            while True:
                try:
                    self._connection_pool.commit()
                    break
                except Exception as e:
                    e: KafkaException
                    if e.args[0].retriable():
                        logger.debug("Retriable error during commit. Retrying...")
                        continue
                    elif e.args[0].txn_requires_abort():
                        logger.debug("Transaction requires abort. Aborting...")
                        self._connection_pool.rollback()

                        logger.debug("Beginning a new transaction...")
                        self._connection_pool.begin()

                        logger.debug("Rewinding the consumer to start over...")
                        rewind_consumer_offsets()
                    else:
                        logger.error(f"Fatal error during commit: {e}")
                        raise
        except Exception as e:
            self._connection_pool.rollback()
            raise e
        
    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        self._connection_pool.close_cursor()
        self._connection_pool.close_all(exclude_calling_thread=True)

    def close(self) -> t.Any:
        """Closes all open connections and releases all allocated resources."""
        self._connection_pool.close_cursor()
        self._connection_pool.close_all()
    
    def replace_query(
        self,
        topics: str, # List of topics separated by comma
        df: DataFrameType,
        **kwargs: t.Any,
    ) -> None:
        topic_list = [tn.strip() for tn in topics.split(',')]
        self.execute(json.dumps({"delete": {"topics": topic_list}}))
        self.insert_append(topics, df, **kwargs)

    def insert_append(
        self,
        topics: str, # List of topics separated by comma
        df: DataFrameType,
        **kwargs: t.Any,
    ) -> None:
        topic_list = [tn.strip() for tn in topics.split(',')]
        df_case_insens_cols = FallbackSet(df.columns)
        if "value" not in df_case_insens_cols:
            raise MissingColumnError("Column named `value` is required in the input DataFrame.")
        
        if "key" in df_case_insens_cols:
            keys = df[df_case_insens_cols.get("key")].map(bytes, na_action="ignore").replace(np.nan, None)
        else:
            keys = [None] * len(df)
        values = df[df_case_insens_cols.get("value")].map(bytes, na_action="ignore").replace(np.nan, None)
        with self.transaction():
            for topic in topic_list:
                request = json.dumps({"produce": {"topic": topic}})
                self.execute(request, payload=list({"key": key, "value": value} for key, value in zip(keys, values)))

    def merge(self, *args, **kwargs) -> None:
        raise EngineAdapterError("Unsupported MERGE operation for Kafka sinks.")

    def ping(self) -> None:
        try:
            self.connection.ping()
        finally:
            self._connection_pool.close() # Instead of close_cursor()

    def columns(
        self, topics: str, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        if self.table_exists(topics):
            return MESSAGE_SCHEMA
        else:
            return {}

    def table_exists(self, topics: str) -> bool:
        topic_list = [tn.strip() for tn in topics.split(',')]
        cursor: KafkaClientCursor = self.cursor
        topic_names = cursor.filter_topics(topic_list)
        return len(topic_names) > 0
    
    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        keyword = keyword.strip()
        cursor: KafkaClientCursor = self.cursor
        data_objects = []
        for topic in cursor.topics:
            if '.' in topic:
                schema: str = topic.split('.')[0]
                table: str = '.'.join(topic.split('.')[1:])
            else:
                schema = ''
                table = topic

            if '.' in keyword:
                schema_pattern = keyword.split('.')[0].lower()
                table_pattern = '.'.join(keyword.split('.')[1:]).lower()
                if schema_pattern and not schema.lower().endswith(schema_pattern):
                    continue
                if table_pattern and not table.lower().startswith(table_pattern):
                    continue
            else:
                if keyword and not keyword.lower() in table.lower():
                    continue

            data_objects.append(DataObject(
                schema=schema,
                name=table,
                type=DataObjectType.TABLE,
            ))
        return data_objects
    
    def determine_high_watermark(self, df: DataFrameType, watermark_field: str, current_watermark_value: t.Any) -> t.Any:
        current_state: t.Dict[str, t.Dict[str, int]] = {}
        if current_watermark_value:
            try:
                if isinstance(current_watermark_value, (str, bytes, bytearray)):
                    loaded = json.loads(current_watermark_value)
                else:
                    loaded = current_watermark_value
                
                if isinstance(loaded, dict):
                    current_state = loaded
            except (json.JSONDecodeError, TypeError):
                logger.warning("Could not parse current watermark value. Starting with a fresh state.")
                current_state = {}

        if isinstance(df, pyspark.sql.DataFrame):
            max_offsets_in_batch = {}
            agg_results = (
                df.groupBy("topic", "partition")
                  .agg(F.max("offset").alias("max_offset"))
                  .collect()
            )
            for row in agg_results:
                max_offsets_in_batch[(row.topic, row.partition)] = row.max_offset
        elif isinstance(df, pd.DataFrame):
            max_offsets_in_batch = df.groupby(['topic', 'partition'])["offset"].max().to_dict()
        else:
            raise UnsupportedDataFrameError(f"DataFrame type not supported for Kafka watermark calculation: {type(df).__name__}")
        
        # Deep copy to avoid modifying the original state dictionary in place
        new_state = defaultdict(dict, {k: v.copy() for k, v in current_state.items()})
        for (topic, partition), max_offset in max_offsets_in_batch.items():
            partition_str = str(partition)
            previous_max = new_state.get(topic, {}).get(partition_str, -1)
            if max_offset > previous_max:
                new_state[topic][partition_str] = int(max_offset)

        return new_state
