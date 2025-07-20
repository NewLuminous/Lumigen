import typing as t

import json
from collections import OrderedDict
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.conf import SparkConf
from sqlglot import exp

from core.engine_context import SQLContextManager
from engines.spark.errors import NotSupportedError
from engines.spark.spark_session import SparkSessionCursor, SparkSessionConnection

logger = logging.getLogger(__name__)


class SparkKafkaCursor(SparkSessionCursor):
    MESSAGE_SCHEMA: t.Dict[str, exp.DataType] = OrderedDict([
        ('key', exp.DataType.build("binary")),
        ('value', exp.DataType.build("binary")),
        ('topic', exp.DataType.build("text")),
        ('partition', exp.DataType.build("int")),
        ('offset', exp.DataType.build("bigint")),
        ('timestamp', exp.DataType.build("timestamp")),
        ('timestampType', exp.DataType.build("int")),
        ('headers', exp.DataType.build("array")),
    ])

    def __init__(self, spark: SparkSession, bootstrap_servers: str, group_id: str, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {}):
        super().__init__(spark)
        self.kafka_options = self._kafka_options_factory(bootstrap_servers, group_id, starting_mode, kafka_config)

    @classmethod
    def _kafka_options_factory(cls, bootstrap_servers: str, group_id: str, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {}):
        kafka_options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            #"kafka.group.id": group_id,
            "startingOffsets": starting_mode,
            "failOnDataLoss": True,
            "includeHeaders": True,
        }

        # Some Kafka params cannot be set and the Kafka source or sink will throw an exception (https://spark.apache.org/docs/3.5.1/structured-streaming-kafka-integration.html)
        FORBIDDEN_KAFKA_PARAMS = {
            "auto.offset.reset",
            "key.deserializer", "value.deserializer", "key.serializer", "value.serializer",
            "enable.auto.commit",
            "interceptor.classes",
        }
        for k, v in kafka_config.items():
            if k in FORBIDDEN_KAFKA_PARAMS:
                logger.warning(f"Kafka param {k} cannot be set.")
            else:
                kafka_options[f"kafka.{k}"] = v

        return kafka_options

    def execute(self, json_query: str, parameters: t.Optional[t.Any] = None) -> None:
        if parameters:
            raise NotSupportedError("Parameterized queries are not supported")
        
        query: dict = json.loads(json_query)
        topics = query.get("topics", [])
        topic_pattern = '|'.join(f"({tn})" for tn in topics)

        self._last_df = self._spark.read.format("kafka") \
            .options(**self.kafka_options) \
            .option("subscribePattern", topic_pattern) \
            .load()
        self._last_output = None
        self._last_output_cursor = 0

        self._stream_df = self._spark.readStream.format("kafka") \
            .options(**self.kafka_options) \
            .option("subscribePattern", topic_pattern) \
            .load()
        
    def fetchdf(self) -> t.Optional[DataFrame]:
        return self._last_df

    def fetchstreamdf(self, **kwargs) -> t.Iterator[t.Optional[DataFrame]]:
        yield self._stream_df


class SparkKafkaConnection(SparkSessionConnection):
    def __init__(self, spark: SparkSession, bootstrap_servers: str, group_id: str, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {}, catalog: t.Optional[str] = None):
        super().__init__(spark, catalog)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.starting_mode = starting_mode
        self.kafka_config = kafka_config

    def cursor(self) -> SparkKafkaCursor:
        super().cursor()
        return SparkKafkaCursor(self.spark, self.bootstrap_servers, self.group_id, self.starting_mode, self.kafka_config)


def connection(app_name: t.Optional[str], conf: SparkConf, bootstrap_servers: str, group_id: str,
               starting_mode: t.Optional[str] = "earliest",
               kafka_config: t.Optional[t.Dict[str, t.Any]] = {},
               catalog: t.Optional[str] = None,
               hive_support: t.Optional[bool] = True,
               ) -> SparkKafkaConnection:
    manager = SQLContextManager()
    spark = manager.get_spark_session(app_name=app_name, conf=conf, hive_support=hive_support)
    return SparkKafkaConnection(spark, bootstrap_servers, group_id, starting_mode, kafka_config, catalog)