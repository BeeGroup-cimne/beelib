from kafka import KafkaProducer
from base64 import b64encode
import pickle
import sys


def create_kafka_producer(kafka_conf):
    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaProducer(bootstrap_servers=servers, value_serializer=lambda v: b64encode(pickle.dumps(v)))


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
