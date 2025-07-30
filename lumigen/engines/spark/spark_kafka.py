from __future__ import annotations
import typing as t
import re
import json
import uuid
import tempfile
import shutil
import logging
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

if t.TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from engines.spark.spark_session import SparkSessionCursor, SparkSessionConnection
from engines.kafka.request_parser import KafkaRequestParser, RequestParseError
from utils import merge_dicts

logger = logging.getLogger(__name__)


def _generate_unique_id(prefix: str="lumigen") -> str:
    return f"{prefix}-{uuid.uuid4()}"

class SparkKafkaCursor(SparkSessionCursor):
    LATEST_OFFSET = -1
    EARLIEST_OFFSET = -2

    def __init__(self, spark: SparkSession, admin: AdminClient, bootstrap_servers: str,
                 group_id: t.Optional[str] = None, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {}):
        super().__init__(spark)
        self._admin = admin
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._starting_mode = starting_mode
        self._kafka_config = kafka_config

    @classmethod
    def _kafka_options_factory(cls, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {}):
        kafka_options = {
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

    def execute(self, request: str, payload: t.Optional[t.Any] = None, **kwargs) -> None:
        try:
            parsed_request = KafkaRequestParser.parse(request)
        except RequestParseError as e:
            logger.error(f"Failed to execute request due to parsing error.", exc_info=e)
            raise

        if parsed_request.consume:
            self._subscribe(
                topics=parsed_request.consume.topics,
                stored_offsets=parsed_request.consume.stored_offsets
            )
        elif parsed_request.produce:
            self._produce(parsed_request.produce.topic, payload)
        elif parsed_request.create:
            self._create_topic(
                topic=parsed_request.create.topic,
                num_partitions=parsed_request.create.num_partitions,
                replication_factor=parsed_request.create.replication_factor,
            )
        elif parsed_request.delete:
            self._delete_topics(topics=parsed_request.delete.topics)

    def _create_topic(self, topic: str, num_partitions: t.Optional[int]=1, replication_factor: t.Optional[int]=1, timeout: float=10.0):
        new_topic = NewTopic(topic, num_partitions, replication_factor)
        create_response = self._admin.create_topics([new_topic], operation_timeout=timeout)
        for topic, future in create_response.items():
            try:
                future.result()  # Block until the operation is complete
                logger.info(f"Topic '{topic}' successfully created.")
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}'.")
                raise e

    def _subscribe(self, topics: t.List[str], stored_offsets: t.Optional[t.Dict[str, dict]] = None):
        topic_pattern = '|'.join(f"({tn})" for tn in topics)

        default_kafka_config = {
            'bootstrap.servers': self._bootstrap_servers,
            #'group.id': self._group_id, # NOTE: Not using for now
        }
        kafka_config = merge_dicts(default_kafka_config, self._kafka_config)
        consumer_config = self._kafka_options_factory(self._starting_mode, kafka_config)
        consumer_config["subscribePattern"] = topic_pattern

        if stored_offsets:
            subscribed_topics = self.filter_topics(topics)
            starting_offsets = {}
            for topic_name, topic_metadata in subscribed_topics.items():
                starting_offsets[topic_name] = {}
                partitions: t.Dict[int, t.Any] = topic_metadata.partitions
                for partition_id in partitions:
                    partition_str = str(partition_id)
                    stored_offset = stored_offsets.get(topic_name, {}).get(partition_str)
                    if stored_offset is None:
                        starting_offsets[topic_name][partition_id] = self.EARLIEST_OFFSET
                    elif stored_offset >= 0:
                        starting_offsets[topic_name][partition_id] = stored_offset + 1
                    else:
                        starting_offsets[topic_name][partition_id] = stored_offset
            
            consumer_config["startingOffsets"] = json.dumps(starting_offsets)

        self._last_df = self._spark.read.format("kafka").options(**consumer_config).load()
        self._stream_df = self._spark.readStream.format("kafka").options(**consumer_config).load()
        self._last_output = None
        self._last_output_cursor = 0

    def fetchdf(self) -> t.Optional[DataFrame]:
        return self._last_df

    def fetchstreamdf(self, **kwargs) -> t.Iterator[t.Optional[DataFrame]]:
        yield self._stream_df

    def _produce(self, topic: str, df: t.Optional[DataFrame] = None):
        default_kafka_config = {
            'bootstrap.servers': self._bootstrap_servers,
        }
        kafka_config = merge_dicts(default_kafka_config, self._kafka_config)
        producer_config = self._kafka_options_factory(self._starting_mode, kafka_config)
        producer_config["topic"] = topic

        if df.isStreaming:
            writer = df.writeStream.format("kafka").options(**producer_config)

            checkpoint_location = self._spark.conf.get("spark.sql.streaming.checkpointLocation", None)
            temp_dir_to_clean = None
            if not checkpoint_location:
                try:
                    temp_dir_to_clean = tempfile.mkdtemp(prefix="spark-tmp-ckpt_")
                    checkpoint_location = f"file://{temp_dir_to_clean}"
                    logger.info(
                        f"No spark.sql.streaming.checkpointLocation configured. "
                        f"Using temporary checkpoint for this write: {checkpoint_location}"
                    )
                except Exception as e:
                    logger.error("Failed to create temporary checkpoint directory for Kafka sink.", exc_info=True)
                    raise RuntimeError("Could not create a temporary directory for Spark checkpointing.") from e
            writer = writer.option("checkpointLocation", checkpoint_location)
            
            try:
                query = writer.outputMode("append").start()
                logger.info(f"Spark streaming Kafka producer '{query.name}' started. Awaiting termination...")
                query.awaitTermination()
                logger.info(f"Spark streaming Kafka producer '{query.name}' terminated.")
            finally:
                if temp_dir_to_clean:
                    try:
                        logger.info(f"Cleaning up temporary checkpoint directory: {temp_dir_to_clean}")
                        shutil.rmtree(temp_dir_to_clean)
                        logger.debug("Temporary directory successfully removed.")
                    except Exception as e:
                        logger.warning(
                            f"Failed to clean up temporary Kafka sink checkpoint directory: {temp_dir_to_clean}. "
                            f"Manual cleanup may be required. Error: {e}"
                        )
        else:
            writer = df.write.format("kafka").options(**producer_config)
            writer.mode("append").save()

    def _delete_topics(self, topics: t.List[str], timeout: float=30.0):
        topics_to_delete = self.filter_topics(topics)
        if len(topics_to_delete) > 0:
            delete_response = self._admin.delete_topics(list(topics_to_delete), operation_timeout=timeout)
            for topic, future in delete_response.items():
                try:
                    future.result()  # Block until the operation is complete
                    logger.info(f"Topic '{topic}' successfully deleted.")
                except Exception as e:
                    logger.error(f"Failed to delete topic '{topic}'.")
                    raise e

    @property
    def topics(self) -> t.Dict[str, t.Any]:
        cluster_metadata = self._admin.list_topics(timeout=10.0)
        if cluster_metadata is None:
            logger.warning("list_topics returned None, cluster might be unavailable.")
            return {}
        
        return cluster_metadata.topics
    
    def filter_topics(self, patterns: t.List[str]) -> t.Dict[str, t.Any]:
        result = {
            topic_name: topic_metadata for topic_name, topic_metadata in self.topics.items() 
            if any(re.fullmatch(pattern, topic_name) for pattern in patterns)
        }
        return result


class SparkKafkaConnection(SparkSessionConnection):
    def __init__(self, spark: SparkSession, bootstrap_servers: str,
                 group_id: t.Optional[str] = None, starting_mode: t.Optional[str] = "earliest", kafka_config: t.Optional[t.Dict[str, t.Any]] = {},
                 catalog: t.Optional[str] = None):
        super().__init__(spark, catalog)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.starting_mode = starting_mode
        self.kafka_config = kafka_config
        self.admin = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })

    def get_admin(self) -> AdminClient:
        return self.admin
    
    def ping(self, timeout: float = 1.0) -> None:
        try:
            future = self.admin.describe_cluster(request_timeout=timeout)
            cluster_metadata = future.result() 
            if cluster_metadata.controller is None:
                raise KafkaException("Ping failed: Cluster metadata returned no controller broker.")

            controller = cluster_metadata.controller
            logger.debug(
                f"Kafka ping successful. Controller ID: {controller.id}, Host: {controller.host}:{controller.port}"
            )
        except KafkaException as e:
            logger.error(f"Kafka ping failed within {timeout}s.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Kafka ping.", exc_info=True)
            raise

    def cursor(self) -> SparkKafkaCursor:
        super().cursor()
        return SparkKafkaCursor(self.spark, self.admin, self.bootstrap_servers, self.group_id, self.starting_mode, self.kafka_config)


def connection(spark: SparkSession, bootstrap_servers: str,
               group_id: t.Optional[str] = None,
               starting_mode: t.Optional[str] = "earliest",
               kafka_config: t.Optional[t.Dict[str, t.Any]] = {},
               catalog: t.Optional[str] = None,
               ) -> SparkKafkaConnection:
    return SparkKafkaConnection(spark, bootstrap_servers, group_id, starting_mode, kafka_config, catalog)