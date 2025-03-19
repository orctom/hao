# -*- coding: utf-8 -*-
# https://github.com/dpkp/kafka-python
"""
####################################################
###########         dependency          ############
####################################################
pip install kafka-python

####################################################
###########         config.yml          ############
####################################################
kafka:
  default:
    hosts:
      - host1:port1
      - host2:port2
      - host3:port3
    group_id: group_id
    client_id: client_id
  some-other:
    hosts:
      - host1:port1
      - host2:port2
      - host3:port3
    group_id: group_id
    client_id: client_id

####################################################
###########          usage              ############
####################################################
from hao.kafka import Kafka
from kafka.errors import CommitFailedError
kafka = Kafka()
kafka = Kafka('some-other')
topic = 'test-hao'

# publish
kafka.publish(topic, 'this is a message')

# consume
consumer = kafka.get_consumer(topic)
for msg in consumer:
    topic = msg.topic
    try:
        payload = msg.value.decode('utf-8')
        LOGGER.info(f'topic: {topic}, payload: {payload}')
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = payload
        LOGGER.info(data)
        consumer.commit()
    except CommitFailedError as e:
        LOGGER.error(e)
    except Exception as e:
        LOGGER.exception(e)

"""
import typing

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

from . import config, jsons, logs

LOGGER = logs.get_logger(__name__)


class Kafka(object):

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self.__conf = config.get(f"kafka.{self.profile}")
        if self.__conf is None:
            raise ValueError(f'no config found for kafka, expecting: `kafka.{self.profile}`')
        self._producer = None

    def __str__(self) -> str:
        return f"hosts: {self.__conf.get('hosts')}, group_id: {self.__conf.get('group_id')}"

    def __repr__(self) -> str:
        return self.__str__()

    def get_consumer(self,
                     topic: typing.Union[str, list],
                     group_id=None,
                     client_id=None,
                     **kwargs):
        topics = [topic] if isinstance(topic, str) else topic
        group_id = group_id or self.__conf.get('group_id')
        client_id = client_id or self.__conf.get('client_id', self.__conf.get('group_id'))
        LOGGER.info(f"[kafka] consumer to topics: {topics}, group_id: {group_id}, client_id: {client_id}")
        args = {
            'bootstrap_servers': self.__conf.get('hosts'),
            'group_id': group_id,
            'client_id': client_id,
            'fetch_max_wait_ms': kwargs.get('fetch_max_wait_ms') or self.__conf.get('fetch_max_wait_ms', 500),
            'request_timeout_ms': kwargs.get('request_timeout_ms') or self.__conf.get('request_timeout_ms', 305_000),
            'max_in_flight_requests_per_connection': kwargs.get('max_in_flight_requests_per_connection') or self.__conf.get('max_in_flight_requests_per_connection', 5),
            'auto_offset_reset': kwargs.get('auto_offset_reset') or self.__conf.get('auto_offset_reset', 'earliest'),
            'enable_auto_commit': kwargs.get('enable_auto_commit') or self.__conf.get('enable_auto_commit', False),
            'auto_commit_interval_ms': kwargs.get('auto_commit_interval_ms') or self.__conf.get('auto_commit_interval_ms', 5_000),
            'max_poll_records': kwargs.get('max_poll_records') or self.__conf.get('max_poll_records', 500),
            'max_poll_interval_ms': kwargs.get('max_poll_interval_ms') or self.__conf.get('max_poll_interval_ms', 300_000),
            'session_timeout_ms': kwargs.get('session_timeout_ms') or self.__conf.get('session_timeout_ms', 10_000),
            'heartbeat_interval_ms': kwargs.get('heartbeat_interval_ms') or self.__conf.get('heartbeat_interval_ms', 3_000),
            'consumer_timeout_ms': kwargs.get('consumer_timeout_ms') or self.__conf.get('consumer_timeout_ms', float('inf')),
            'connections_max_idle_ms': kwargs.get('connections_max_idle_ms') or self.__conf.get('connections_max_idle_ms', 540_000),
            'api_version': self.__conf.get('api_version'),
        }
        return KafkaConsumer(*topics, **args)

    def get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.__conf.get('hosts'),
                api_version=self.__conf.get('api_version')
            )
        return self._producer

    def publish(self, topic, message, key=None, headers=None, partition=None, timestamp_ms=None, flush: bool = False):
        if isinstance(message, str):
            payload = message.encode()
        elif isinstance(message, dict):
            payload = jsons.dumps(message).encode()
        else:
            LOGGER.warning(f'Unsupported message type: {type(message)}: {message}')
            return
        try:
            LOGGER.debug(f"sending payload: {payload}")
            producer = self.get_producer()
            future = producer.send(topic, payload, key, headers, partition, timestamp_ms)
            if flush:
                producer.flush(10)
            return future
        except (KafkaTimeoutError, NoBrokersAvailable) as err:
            LOGGER.error(f'Failed to send data to {topic}, payload: {payload}')
            LOGGER.error(err)

    def flush(self, timeout=10):
        self.get_producer().flush(timeout)
