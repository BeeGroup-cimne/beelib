import json
import re
import time
from typing import Any
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from base64 import b64encode, b64decode
import pickle
import sys
import warnings

class BeeMessage:
    topic = None
    partition = None
    offset = None
    key = None
    value = None
    headers = None
    error = None
    timestamp = None
    latency = None
    leader_epoch = None


class BeeKafka(object):

    def __init__(self, kafka_conf=None, **kwargs):
        new_kwargs = {}
        for k, v in kwargs.items():
            new_kwargs[k.replace("_", ".")] = v
        self.kafka_conf = dict(kafka_conf if kafka_conf else {}, **new_kwargs)

    @staticmethod
    def __pickle_encoder__(v):
        return b64encode(pickle.dumps(v))

    @staticmethod
    def __pickle_decoder__(v):
        return pickle.loads(b64decode(v))

    @staticmethod
    def __json_encoder__(v):
        return json.dumps(v).encode("utf-8")

    @staticmethod
    def __json_decoder__(v):
        return json.loads(v.decode("utf-8"))

    @staticmethod
    def __plain_decoder_encoder__(v):
        return v


class BeeProducer(BeeKafka):

    def __init__(self, kafka_conf, **kwargs):
        value_serializer = kwargs.pop('value_serializer', None)
        key_serializer = kwargs.pop('key_serializer', None)
        super().__init__(kafka_conf, **kwargs)
        self.producer = Producer(self.kafka_conf)
        self.value_serializer = value_serializer if value_serializer else self.__plain_decoder_encoder__
        self.key_serializer = key_serializer if key_serializer else self.__plain_decoder_encoder__

    def send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        v = self.value_serializer(value)
        k = self.key_serializer(key)
        kwargs = {
            "key": k,
            "value": v,
            "headers": headers,
        }
        if partition:
            kwargs["partition"] = partition
        if timestamp_ms:
            kwargs["timestamp"] = timestamp_ms
        self.producer.produce(topic, **kwargs)


class BeeConsumer(BeeKafka):
    def __init__(self, kafka_conf, **kwargs):
        value_deserializer = kwargs.pop('value_deserializer', None)
        key_deserializer = kwargs.pop('key_deserializer', None)
        super().__init__(kafka_conf, **kwargs)
        self.refresh_ms = self.kafka_conf['topic.metadata.refresh.interval.ms'] \
            if 'topic.metadata.refresh.interval.ms' in self.kafka_conf else 30
        self.consumer = Consumer(self.kafka_conf)
        self.value_deserializer = value_deserializer if value_deserializer else self.__plain_decoder_encoder__
        self.key_deserializer = key_deserializer if key_deserializer else self.__plain_decoder_encoder__
        self.pattern = None
        self.topics = []
        self.listeners = None

    def __subscribe_with_pattern__(self):
        current_topics = [t for t in self.consumer.list_topics(timeout=10).topics.keys() if self.pattern.match(t)]
        diff = set(current_topics).symmetric_difference(set(self.topics))
        if diff:
            self.topics = list(set(current_topics))
            self.consumer.subscribe(self.topics)

    def subscribe(self, topics: list = (), pattern: str = None, listener: Any = None):
        if topics and pattern:
            raise Exception("Only one of topics and pattern can be specified")
        if topics:
            self.topics = topics
            self.consumer.subscribe(topics)
        if pattern:
            self.pattern = re.compile(pattern)
            self.__subscribe_with_pattern__()
        self.listeners = listener

    def __iter__(self):
        if not self.topics and not self.pattern:
            raise Exception("No topics or pattern specified")
        last_refresh = 0
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg:
                    if msg.error():
                        if msg.error().code() in (
                                KafkaError._PARTITION_EOF,
                                KafkaError.UNKNOWN_TOPIC_OR_PART,
                        ):
                            continue
                        else:
                            warnings.warn(f"Fatal Error: {msg.error()}")
                            continue
                    ret = BeeMessage()
                    ret.topic = msg.topic()
                    ret.value = self.value_deserializer(msg.value())
                    ret.key = self.key_deserializer(msg.key())
                    ret.offset = msg.offset()
                    ret.partition = msg.partition()
                    ret.timestamp_ms = msg.timestamp()
                    ret.headers = msg.headers()
                    ret.error = msg.error()
                    ret.latency = msg.latency()
                    ret.leader_epoch = msg.leader_epoch()
                    yield ret

                if self.pattern and time.time() - last_refresh > self.refresh_ms:
                    self.__subscribe_with_pattern__()
                    last_refresh = time.time()
            except Exception as e:
                warnings.warn(f"Fatal Error: {e}")


def create_kafka_producer(kafka_conf, encoding="JSON", **kwargs):
    if encoding == "PLAIN":
        encoder = BeeKafka.__plain_decoder_encoder__
    elif encoding == "PICKLE":
        encoder = BeeKafka.__pickle_encoder__
    elif encoding == "JSON":
        encoder = BeeKafka.__json_encoder__
    else:
        raise NotImplementedError("Unknown encoding")
    return BeeProducer(kafka_conf, value_serializer=encoder, **kwargs)


def create_kafka_consumer(kafka_conf, encoding="JSON", **kwargs):
    if encoding == "PLAIN":
        decoder = BeeKafka.__plain_decoder_encoder__
    elif encoding == "PICKLE":
        decoder = BeeKafka.__pickle_decoder__
    elif encoding == "JSON":
        decoder = BeeKafka.__json_decoder__
    else:
        raise NotImplementedError("Unknown encoding")
    return BeeConsumer(kafka_conf, value_deserializer=decoder, **kwargs)


def send_to_kafka(producer, topic, key, data, **kwargs):
    try:
        kafka_message = {
            "data": data
        }
        kafka_message.update(kwargs)
        if key:
            producer.send(topic, key=key.encode('utf-8'), value=kafka_message)
        else:
            producer.send(topic, value=kafka_message)
    except Exception as e:
        print(e, file=sys.stderr)
