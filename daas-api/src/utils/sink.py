import os
import json

from kafka import KafkaProducer

KAFKA_HOST = os.getenv("KAFKA_HOST", "0.0.0.0")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", 9092))


def sink_data(key: str, message: str):
    prod = KafkaProducer(
        bootstrap_servers=[f"{KAFKA_HOST}:{KAFKA_PORT}"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    prod.send(key, value=message)
