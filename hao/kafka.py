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
      - kafka001.bl-ai.com:59092
      - kafka002.bl-ai.com:59092
      - kafka003.bl-ai.com:59092
    group_id: bidding-hao
    client_id: bidding-hao
  some-other:
    hosts:
      - kafka001.bl-ai.com:59092
      - kafka002.bl-ai.com:59092
      - kafka003.bl-ai.com:59092
    group_id: bidding-hao
    client_id: bidding-hao

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
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError

from . import logs, config, jsons

LOGGER = logs.get_logger(__name__)


class Kafka(object):

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self._conf = config.get(f"kafka.{self.profile}")
        if self._conf is None:
            raise ValueError(f'no config found for kafka, expecting: `kafka.{self.profile}`')
        self._producer = None

    def get_consumer(self,
                     topic: typing.Union[str, list],
                     enable_auto_commit=False,
                     auto_commit_interval_ms=1_000,
                     max_poll_records=500):
        topics = [topic] if isinstance(topic, str) else topic
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self._conf.get('hosts'),
            group_id=self._conf.get('group_id'),
            client_id=self._conf.get('client_id', self._conf.get('group_id')),
            auto_offset_reset=self._conf.get('auto_offset_reset', 'earliest'),
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            session_timeout_ms=self._conf.get('session_timeout_ms', 10_000),
            max_poll_records=max_poll_records,
            api_version=(1, 1, 0)
        )

    def get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._conf.get('hosts'),
                api_version=(1, 1, 0)
            )
        return self._producer

    def publish(self, topic, message):
        message_type = type(message)
        if message_type == str:
            payload = message.encode()
        elif message_type == dict:
            payload = jsons.dumps(message).encode()
        else:
            LOGGER.warning(f'Unsupported message type: {message_type}: {message}')
            return
        try:
            LOGGER.debug(f"sending payload: {payload}")
            return self.get_producer().send(topic, payload)
        except (KafkaTimeoutError, NoBrokersAvailable) as err:
            LOGGER.error(f'Failed to send data to {topic}, payload: {payload}')
            LOGGER.error(err)

