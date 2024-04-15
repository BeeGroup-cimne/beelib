import json
from kafka import KafkaProducer, KafkaConsumer
from base64 import b64encode, b64decode
import pickle
import sys


def __pickle_encoder__(v):
    return b64encode(pickle.dumps(v))


def __pickle_decoder__(v):
    return pickle.loads(b64decode(v))


def __json_encoder__(v):
    return json.dumps(v).encode("utf-8")


def __json_decoder__(v):
    return json.loads(v.decode("utf-8"))


def __plain_decoder_encoder(v):
    return v


def create_kafka_producer(kafka_conf, encoding="JSON", **kwargs):
    if encoding == "PLAIN":
        encoder = __plain_decoder_encoder
    elif encoding == "PICKLE":
        encoder = __pickle_encoder__
    elif encoding == "JSON":
        encoder = __json_encoder__
    else:
        raise NotImplementedError("Unknown encoding")
    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaProducer(bootstrap_servers=servers, value_serializer=encoder, **kwargs)


def create_kafka_consumer(kafka_conf, encoding="JSON", **kwargs):
    if encoding == "PLAIN":
        decoder = __plain_decoder_encoder()
    elif encoding == "PICKLE":
        decoder = __pickle_decoder__
    elif encoding == "JSON":
        decoder = __json_decoder__
    else:
        raise NotImplementedError("Unknown encoding")

    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaConsumer(bootstrap_servers=servers, value_deserializer=decoder, **kwargs)


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
