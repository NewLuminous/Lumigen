"""Contains KafkaEngineAdapter."""

from __future__ import annotations

import typing as t

import logging
import contextlib
from sqlglot import exp
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
)
import pandas as pd
from confluent_kafka import KafkaException

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import DF

from core.engine_adapter.mixins import EngineAdapterMixin, FetchStreamMixin
from utils.errors import EngineAdapterError, MissingColumnError

logger = logging.getLogger(__name__)


class KafkaEngineAdapter(FetchStreamMixin, EngineAdapterMixin):
    DIALECT: str = "kafka"

    def fetch_batch_df(
        self,
        topic: t.Union[exp.Expression, str],
        batch_size: int = None,
        quote_identifiers: bool = False,
    ) -> t.Iterator[DF]:
        batches = self.fetchbatch(topic, batch_size, quote_identifiers=quote_identifiers)
        for batch in batches:
            logger.info(f"Fetched {len(batch)} Kafka messages")
            yield pd.DataFrame(batch, columns=['key', 'value', 'topic', 'partition', 'offset', 'timestamp', 'timestampType', 'headers'])

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
                except KafkaException as e:
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
    
    def replace_query(
        self,
        topics: str, # List of topics separated by comma
        df: DF,
        **kwargs: t.Any,
    ) -> None:
        topic_list = [tn.strip() for tn in topics.split(',')]
        topics_to_delete = self._connection_pool.get().filter_topics(topic_list)
        if len(topics_to_delete) > 0:
            delete_response = self._connection_pool.get().get_admin().delete_topics(topics_to_delete)
            for topic, future in delete_response.items():
                try:
                    future.result()  # Block until the operation is complete
                    logger.info(f"Topic '{topic}' successfully deleted.")
                except Exception as e:
                    logger.error(f"Failed to delete topic '{topic}'.")
                    raise e
                
        self.insert_append(topics, df, **kwargs)

    def insert_append(
        self,
        topics: str, # List of topics separated by comma
        df: DF,
        **kwargs: t.Any,
    ) -> None:
        def delivery_report(err, msg):
            if err is not None:
                logger.debug(f"Message delivery failed: {err}")

        topic_list = [tn.strip() for tn in topics.split(',')]
        if "value" not in df.columns:
            raise MissingColumnError("Column named `value` is required in the input DataFrame.")
        
        keys = df["key"].map(bytes, na_action="ignore") if "key" in df.columns else [None] * len(df)
        values = df["value"].map(bytes, na_action="ignore")
        with self.transaction():
            for topic in topic_list:
                for key, value in zip(keys, values):
                    self._connection_pool.get().get_producer().produce(topic, key=key, value=value, callback=delivery_report)

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
        return self.cursor.MESSAGE_SCHEMA

    def table_exists(self, topics: str) -> bool:
        topic_list = [tn.strip() for tn in topics.split(',')]
        topic_names = self._connection_pool.get().filter_topics(topic_list)
        return len(topic_names) > 0
    
    def get_all_data_objects(self, keyword: str="") -> t.List[DataObject]:
        keyword = keyword.strip()
        topics = self._connection_pool.get().topics
        data_objects = []
        for topic in topics:
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