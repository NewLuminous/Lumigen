from __future__ import annotations
import typing as t
import logging
import re
import json
import time
import socket
import uuid
from datetime import datetime
from collections import defaultdict, OrderedDict
from threading import Lock
from sqlglot import exp
from sqlglot.helper import ensure_list
from pandas import DataFrame
from confluent_kafka import Consumer, Producer, Message, KafkaException, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from confluent_kafka.admin import AdminClient, NewTopic

from utils import merge_dicts
from engines.kafka.request_parser import KafkaRequestParser, RequestParseError

logger = logging.getLogger(__name__)


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

class KafkaClientCursor:
    LATEST_OFFSET = -1
    EARLIEST_OFFSET = -2

    def __init__(self, admin: AdminClient, bootstrap_servers: str, group_id: str, transactional_id: str,
                 starting_mode: str, shared_config: t.Dict[str, t.Any]):
        self._admin = admin
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._transactional_id = transactional_id
        self._starting_mode = starting_mode
        self._shared_config = shared_config
        
        # Lazy initialization attributes and their corresponding thread locks
        self._consumer: t.Optional[Consumer] = None
        self._consumer_lock = Lock()

        self._producer: t.Optional[Producer] = None
        self._producer_lock = Lock()

        self._rebalance_in_progress = False
        self._rebalance_lock = Lock()

        self._is_in_transaction = False
        self._producer_transaction_started = False
        
        # Cursor state
        self._topic_patterns = []
        self._assignments: t.Dict[str, t.List] = defaultdict(list)
        self._last_consumed_offsets = defaultdict(dict)
        self._last_msg_ts = time.time()
        self._no_msg_cnt = 0
        self._no_msg_log_interval = 3600

    @property
    def consumer(self) -> Consumer:
        if self._consumer is None:
            with self._consumer_lock:
                if self._consumer is None:
                    consumer_config = {
                        'bootstrap.servers': self._bootstrap_servers,
                        'group.id': self._group_id,
                        'auto.offset.reset': self._starting_mode,
                        'enable.auto.commit': False,
                        'isolation.level': 'read_committed',
                    }
                    consumer_config = merge_dicts(consumer_config, self._shared_config)
                    logger.info("Consumer configuration: \n" + json.dumps(consumer_config, indent=2))
                    self._consumer = Consumer(consumer_config)
        return self._consumer
    
    @property
    def producer(self) -> Producer:
        if self._producer is None:
            with self._producer_lock:
                if self._producer is None:
                    producer_config = {
                        'bootstrap.servers': self._bootstrap_servers,
                        'transactional.id': self._transactional_id,
                        'enable.idempotence': True,
                    }
                    producer_config = merge_dicts(producer_config, self._shared_config)
                    logger.info("Producer configuration: \n" + json.dumps(producer_config, indent=2))
                    self._producer = Producer(producer_config)

                    logger.info("Initializing producer transactions...")
                    self._producer.init_transactions(30.0)
        return self._producer
    
    def begin(self) -> None:
        if self._is_in_transaction:
            logger.debug("begin() called on an already active transaction. Ignoring.")
            return
        
        # Lazy Kafka Producer initialization
        self._is_in_transaction = True
        self._producer_transaction_started = False
        logger.debug("Logical transaction started. Kafka producer transaction will start on first write.")

    def commit(self, timeout: float=30.0) -> None:
        if not self._is_in_transaction:
            logger.warning("commit() called without an active transaction. Ignoring.")
            return
        
        try:
            if self._producer is not None and self._producer_transaction_started:
                try:
                    self.producer.commit_transaction(timeout)
                    logger.debug("Kafka transaction committed successfully.")
                except KafkaException as e:
                    logger.error(f"Failed to commit Kafka transaction within {timeout}s.", exc_info=e)
                    self.rollback(timeout)
                    raise
            else:
                logger.debug("No producer activity in this transaction. Nothing to commit.")
        finally:
            self._is_in_transaction = False
            self._producer_transaction_started = False

    def rollback(self, timeout: float = 10.0) -> None:
        if not self._is_in_transaction:
            logger.warning("rollback() called without an active transaction. Ignoring.")
            return
        
        try:
            if self._producer is not None and self._producer_transaction_started:
                self.producer.abort_transaction(timeout)
                logger.warning("Kafka transaction rolled back.")
            else:
                logger.debug("No producer activity in this transaction. Nothing to roll back.")
        except KafkaException as e:
            logger.error("Exception during transaction abort.", exc_info=e)
            raise
        finally:
            self._is_in_transaction = False
            self._producer_transaction_started = False

    def close(self) -> None:
        if self._is_in_transaction:
            logger.warning("Cursor closed with an active transaction. Attempting to roll back.")
            self.rollback()

        if self._consumer is not None:
            self._consumer.close()
            logger.info("Consumer terminated.")

        if self._producer is not None:
            remaining_messages = self._producer.flush(30.0) 
            if remaining_messages > 0:
                logger.warning(f"Producer flush timed out. {remaining_messages} messages may be lost.")

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
            message_payload = payload if payload is not None else parsed_request.produce.messages or {}
            messages = ensure_list(message_payload)
            msg: dict
            for msg in messages:
                self._produce(
                    topic=parsed_request.produce.topic,
                    key=msg.get("key"),
                    value=msg.get("value")
                )
        elif parsed_request.create:
            self._create_topic(
                topic=parsed_request.create.topic,
                num_partitions=parsed_request.create.num_partitions,
                replication_factor=parsed_request.create.replication_factor,
            )
        elif parsed_request.delete:
            self._delete_topics(topics=parsed_request.delete.topics)
        elif parsed_request.clear:
            self._clear_topics(topics=parsed_request.clear.topics)
        else:
            raise NotImplementedError()
        
    def _subscribe(self, topics: t.List[str], stored_offsets: t.Optional[t.Dict[str, dict]] = None):
        self._topic_patterns = topics
        self._last_consumed_offsets = defaultdict(dict)
        if stored_offsets:
            for topic_name, partitions in stored_offsets.items():
                for partition_str, offset in partitions.items():
                    self._last_consumed_offsets[topic_name][int(partition_str)] = offset

        self.consumer.subscribe(
            self._topic_patterns,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_revoke,
        )
        logger.info("Subscribed to topic(s): " + ", ".join(topic for topic in self.subscriptions))

    def _on_assign(self, consumer: Consumer, partitions: t.List[TopicPartition]):
        with self._rebalance_lock:
            logger.info(f"Rebalance: Partitions assigned: {[f'{p.topic}:{p.partition}' for p in partitions]}")
            self._assignments = defaultdict(list)
            for p in partitions:
                self._assignments[p.topic].append(p.partition)
            self._rebalance_in_progress = True

    def _handle_rebalance_seek(self):
        """
        This method is called by the main thread after a rebalance has been detected.
        It performs the seek operations safely.
        """
        with self._rebalance_lock:
            if not self._rebalance_in_progress:
                return # Another thread might have handled it, or it's a false positive.
            
            logger.info("Handling post-rebalance seek operations...")
            for topic, partitions in self._assignments.items():
                for partition in partitions:
                    topic_partition = TopicPartition(topic, partition)
                    last_consumed_offset = self._last_consumed_offsets[topic].get(partition)
                    if last_consumed_offset is None or last_consumed_offset == self.EARLIEST_OFFSET:
                        topic_partition.offset = OFFSET_BEGINNING
                    elif last_consumed_offset == self.LATEST_OFFSET:
                        topic_partition.offset = OFFSET_END
                    elif last_consumed_offset >= 0:
                        topic_partition.offset = last_consumed_offset + 1
                    else:
                        topic_partition.offset = last_consumed_offset

                    try:
                        logger.debug(f"Seeking partition {topic}:{partition} to offset {topic_partition.offset}")
                        self.consumer.seek(topic_partition)
                    except KafkaException as e:
                        logger.error(f"CRITICAL: seek() failed for {topic}:{partition}.", exc_info=e)
                        raise e
            
            logger.info("Post-rebalance seek operations complete.")
            self._rebalance_in_progress = False

    def _on_revoke(self, consumer: Consumer, partitions: t.List[TopicPartition]):
        with self._rebalance_lock:
            logger.info(f"Rebalance: Partitions revoked: {[f'{p.topic}:{p.partition}' for p in partitions]}")
            self._assignments.clear()
        
    def _produce(self, topic: str, key: t.Optional[t.Union[str, bytes]] = None, value: t.Optional[t.Union[str, bytes]] = None):
        if self._is_in_transaction and not self._producer_transaction_started:
            logger.debug("First write detected. Starting physical Kafka transaction.")
            self.producer.begin_transaction()
            self._producer_transaction_started = True

        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"FATAL: Message delivery failed: {err}.")

        self.producer.produce(topic, value=value, key=key, callback=delivery_report)

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
                
    def _clear_topics(self, topics: t.List[str], timeout: float=30.0):
        topics_to_clear = self.filter_topics(topics)
        if len(topics_to_clear) > 0:
            topic_partitions_to_clear = []
            for topic_name, topic_metadata in topics_to_clear.items():
                topic_partitions_to_clear.extend(
                    TopicPartition(topic=topic_name, partition=partition_id, offset=OFFSET_END)
                    for partition_id in topic_metadata.partitions
                )
            response = self._admin.delete_records(topic_partitions_to_clear, operation_timeout=timeout)
            for topic_partition, future in response.items():
                try:
                    future.result()  # Block until the operation is complete
                    logger.info(f"Messages of topic '{topic_partition.topic}' partition {topic_partition.partition} successfully cleared.")
                except Exception as e:
                    logger.error(f"Failed to clear messages of topic '{topic_partition.topic}' partition {topic_partition.partition}.")
                    raise e

    @property
    def topics(self) -> t.Dict[str, t.Any]:
        cluster_metadata = self._admin.list_topics(timeout=10.0)
        if cluster_metadata is None:
            logger.warning("list_topics returned None, cluster might be unavailable.")
            return {}
        
        return cluster_metadata.topics
    
    def filter_topics(self, patterns: t.List[str]) -> t.Dict[str, t.Any]:
        all_topics = self.topics
        result = {
            topic_name: topic_metadata for topic_name, topic_metadata in all_topics.items() 
            if any(re.fullmatch(pattern, topic_name) for pattern in patterns)
        }
        return result
    
    @property
    def subscriptions(self) -> t.List[str]:
        return list(topic for topic in self.filter_topics(self._topic_patterns))
    
    def _get_partitions(self, topic: str) -> t.Dict[int, t.Any]:
        return self.topics[topic].partitions

    def _get_watermark_offsets(self, topic: str, partition: int) -> t.Tuple[int, int]:
        topic_partition = TopicPartition(topic=topic, partition=partition)
        low, high = self.consumer.get_watermark_offsets(topic_partition, timeout=10.0)
        return low, high
    
    def _get_last_offset(self, topic: str, partition: int) -> int:
        low, high = self._get_watermark_offsets(topic, partition)
        return high - 1
    
    def _get_last_offsets(self, topic: str) -> t.Dict[int, int]:
        return {partition: self._get_last_offset(topic, partition) for partition in self._get_partitions(topic)}

    def _load_message(self, msg: t.Optional[Message]) -> t.Optional[t.Tuple]:
        if msg is None:
            self._no_msg_cnt += 1
            if self._no_msg_cnt == 1:
                self._last_msg_ts = time.time()
            elif self._no_msg_cnt % self._no_msg_log_interval == 0:
                logger.info(f"No message consumed during last {(time.time()-self._last_msg_ts):.6f} seconds")
            return None
        else:
            self._no_msg_cnt = 0

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")

        if len(self._last_consumed_offsets) == 0:
            logger.debug("First Kafka message received successfully")
        self._last_consumed_offsets[msg.topic()][msg.partition()] = msg.offset()
        
        ts_type, ts_ms = msg.timestamp()
        dt_object = None
        if ts_ms is not None and ts_ms >= 0:
            dt_object = datetime.fromtimestamp(ts_ms / 1000.0)
        data = (msg.key(), msg.value(), msg.topic(), msg.partition(), msg.offset(), dt_object, ts_type, msg.headers())
        return data
    
    def _consumed_all(self) -> bool:
        for topic in self._assignments:
            for partition in self._assignments[topic]:
                low_offset, high_offset = self._get_watermark_offsets(topic, partition)
                consumed_offset = self._last_consumed_offsets[topic].get(partition, low_offset - 1)
                last_offset = high_offset - 1
                if consumed_offset < last_offset:
                    return False
        return True

    def fetchmany(self, size: int = 1, at_least_one: bool = False) -> t.List[t.Tuple]:
        batch = []
        last_checked = False
        while True:
            msg = self.consumer.poll(1.0)

            # A poll() call is what triggers the on_revoke/on_lost/on_assign callback to run in the background.
            if self._rebalance_in_progress:
                self._handle_rebalance_seek()
                continue
            
            if msg is None and not last_checked:
                last_checked = True
                if at_least_one and len(batch) == 0:
                    continue
                if self._consumed_all():
                    break

            data = self._load_message(msg)
            if data is not None:
                batch.append(data)
                last_checked = False
                if len(batch) >= size:
                    break
        return batch
    
    def fetchone(self) -> t.Optional[t.Tuple]:
        return self.fetchmany(size=1, at_least_one=True)

    def fetchall(self) -> t.List[t.Tuple]:
        return self.fetchmany(size=float('inf'), at_least_one=False)

    def fetchdf(self) -> t.Optional[DataFrame]:
        batch = self.fetchall()
        return DataFrame(batch, columns=list(MESSAGE_SCHEMA.keys()))
    
    def fetchstream(self, size: int = 1) -> t.Iterator[t.List[t.Tuple]]:
        while True:
            batch = self.fetchmany(size=size, at_least_one=True)
            if not batch:
                break

            yield batch

    def fetchstreamdf(self, size: int = 1) -> t.Iterator[t.Optional[DataFrame]]:
        for batch in self.fetchstream(size=size):
            yield DataFrame(batch, columns=list(MESSAGE_SCHEMA.keys()))

def _generate_unique_id(prefix: str="lumigen") -> str:
    return f"{prefix}-{uuid.uuid4()}"

class KafkaClientConnection:
    _SHARED_RANDOM_GROUP_ID = _generate_unique_id()

    def __init__(self, bootstrap_servers: str,
                 group_id: t.Optional[str] = None, starting_mode: t.Optional[str] = "earliest", config: t.Optional[t.Dict[str, t.Any]] = {}):
        self.bootstrap_servers = bootstrap_servers
        #self.group_id = group_id or KafkaClientConnection._SHARED_RANDOM_GROUP_ID
        self.group_id = KafkaClientConnection._SHARED_RANDOM_GROUP_ID # NOTE: Not using provided group id for now
        self.transactional_id = _generate_unique_id()
        self.starting_mode = starting_mode
        self.config = config

        self._admin: t.Optional[AdminClient] = None
        self._admin_lock = Lock()

    @property
    def admin(self) -> AdminClient:
        if self._admin is None:
            with self._admin_lock:
                if self._admin is None:
                    self._admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        return self._admin

    def get_admin(self) -> AdminClient:
        return self.admin
    
    def ping(self, timeout: float = 1.0) -> None:
        try:
            cluster_metadata = self.admin.list_topics(topic="__ping_check__", timeout=timeout)
            if not cluster_metadata.brokers:
                raise KafkaException("Ping failed: No brokers found in cluster metadata.")
            
            for broker in cluster_metadata.brokers.values():
                try:
                    socket.getaddrinfo(broker.host, broker.port)
                except socket.gaierror:
                    raise KafkaException(
                        f"Kafka connection ping failed: Failed to resolve advertised broker "
                        f"'{broker.host}:{broker.port}'. Name or service not known."
                    )

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

    def cursor(self) -> KafkaClientCursor:
        """Creates a new cursor, which will manage its own consumer and producer."""
        return KafkaClientCursor(
            admin=self.admin,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            transactional_id=self.transactional_id,
            starting_mode=self.starting_mode,
            shared_config=self.config,
        )

    def close(self) -> None:
        """Closes the connection object. Cursors must be closed separately."""
        pass


def connection(bootstrap_servers: str,
               group_id: t.Optional[str] = None, starting_mode: t.Optional[str] = "earliest", config: t.Optional[t.Dict[str, t.Any]] = {}) -> KafkaClientConnection:
    return KafkaClientConnection(bootstrap_servers, group_id, starting_mode, config)