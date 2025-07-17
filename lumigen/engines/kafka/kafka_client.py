import logging
import typing as t

import re
import json
import time
from collections import OrderedDict
from confluent_kafka import Consumer, Producer, Message, TopicPartition
from confluent_kafka.admin import AdminClient
from pandas import DataFrame
from sqlglot import exp

from utils import merge_dicts
from utils.parsing import extract_main_table

logger = logging.getLogger(__name__)


class KafkaClientCursor:
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

    def __init__(self, consumer: Consumer):
        self._consumer = consumer
        self._topic_patterns = []
        self._last_consumed_msg = dict()
        self._last_msg_ts = time.time()
        self._no_msg_cnt = 0
        self._no_msg_log_interval = 3600

    def execute(self, query: str, parameters: t.Optional[t.Any] = None) -> None:
        try:
            topics = extract_main_table(query, quoted=False)
        except:
            topics = query
        topic_list = [tn.strip() for tn in topics.split(',')]
        self._consumer.subscribe(topic_list)
        self._topic_patterns = topic_list
        logger.info("Subscribed to topic(s): " + ", ".join(self.subscriptions))

    @property
    def topics(self) -> t.List[str]:
        metadata = self._consumer.list_topics()
        all_topics = metadata.topics.keys()
        return list(all_topics)
    
    @property
    def subscriptions(self) -> t.List[str]:
        subscribed_topics = [topic for topic in self.topics if any(re.fullmatch(pattern, topic) for pattern in self._topic_patterns)]
        return subscribed_topics

    def _get_last_offset(self, topic: str, partition: int):
        topic_partition = TopicPartition(topic=topic, partition=partition)
        low, high = self._consumer.get_watermark_offsets(topic_partition)
        return high - 1
    
    def _get_last_offsets(self, topic: str, partition: t.Optional[int] = None):
        partitions = self._consumer.list_topics(topic).topics[topic].partitions
        return [self._get_last_offset(topic, partition) for partition in partitions]
    
    def _is_last_message(self, msg: Message):
        last_offset = self._get_last_offset(msg.topic(), partition=msg.partition())
        return last_offset < 0 or msg.offset() >= last_offset

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

        if len(self._last_consumed_msg) == 0:
            logger.debug("First Kafka message received successfully")
        self._last_consumed_msg[msg.topic()] = msg
        data = (msg.key(), msg.value(), msg.topic(), msg.partition(), msg.offset(), msg.timestamp()[1], msg.timestamp()[0], msg.headers())
        return data
    
    def _consumed_all(self) -> bool:
        for topic in self.subscriptions:
            if topic in self._last_consumed_msg:
                if not self._is_last_message(self._last_consumed_msg[topic]):
                    return False
            else:
                last_offsets = self._get_last_offsets(topic)
                if any(last_offset >= 0 for last_offset in last_offsets): # non-empty topic
                    return False
        return True

    def fetchmany(self, size: int = 1, at_least_one: bool = False) -> t.List[t.Tuple]:
        batch = []
        last_checked = False
        while True:
            msg = self._consumer.poll(1.0)
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

    def close(self) -> None:
        pass

    def fetchdf(self) -> t.Optional[DataFrame]:
        batch = self.fetchall()
        return DataFrame(batch, columns=list(KafkaClientCursor.MESSAGE_SCHEMA.keys()))
    
    def fetchstream(self, size: int = 1) -> t.Iterator[t.List[t.Tuple]]:
        while True:
            batch = self.fetchmany(size=size, at_least_one=True)
            if not batch:
                break

            yield batch

    def fetchstreamdf(self, size: int = 1) -> t.Iterator[t.Optional[DataFrame]]:
        for batch in self.fetchstream(size=size):
            yield DataFrame(batch, columns=list(KafkaClientCursor.MESSAGE_SCHEMA.keys()))


class KafkaClientConnection:
    def __init__(self, bootstrap_servers: str, group_id: str, starting_mode: t.Optional[str] = "earliest", config: t.Optional[t.Dict[str, t.Any]] = {}):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.starting_mode = starting_mode
        self.config = config
        self.admin = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
        
        consumer_config = merge_dicts({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': starting_mode,
            'enable.auto.commit': False
        }, config)
        logger.info("Consumer configuration: \n" + json.dumps(consumer_config, indent=2))
        self.consumer = Consumer(consumer_config)

        producer_config = merge_dicts({
            'bootstrap.servers': bootstrap_servers,
            'transactional.id': group_id
        }, config)
        logger.info("Producer configuration: \n" + json.dumps(producer_config, indent=2))
        self.producer = Producer(producer_config)
        self.producer.init_transactions()

    def get_admin(self) -> AdminClient:
        return self.admin
    
    def get_consumer(self) -> Consumer:
        return self.consumer
    
    def get_producer(self) -> Producer:
        return self.producer
    
    @property
    def topics(self) -> t.List[str]:
        metadata = self.admin.list_topics()
        all_topics = metadata.topics.keys()
        return list(all_topics)
    
    def filter_topics(self, patterns: t.List[str]) -> t.List[str]:
        topics = [topic for topic in self.topics if any(re.fullmatch(pattern, topic) for pattern in patterns)]
        return topics
    
    def ping(self) -> None:
        self.consumer.list_topics(None, 1)

    def cursor(self) -> KafkaClientCursor:
        return KafkaClientCursor(self.consumer)
    
    def begin(self) -> None:
        self.producer.begin_transaction()

    def commit(self, timeout: float=10.0) -> None:
        #self.consumer.commit() # TODO
        self.producer.commit_transaction(timeout)

    def rollback(self) -> None:
        self.producer.abort_transaction()

    def close(self) -> None:
        self.consumer.close()
        logger.info("Consumer terminated.")


def connection(bootstrap_servers: str, group_id: str, starting_mode: t.Optional[str] = "earliest", config: t.Optional[t.Dict[str, t.Any]] = {}) -> KafkaClientConnection:
    return KafkaClientConnection(bootstrap_servers, group_id, starting_mode, config)