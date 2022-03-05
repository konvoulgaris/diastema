import os
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "0.0.0.0")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_POTR", 5672))


def send_message(key: str, message: str, host=RABBITMQ_HOST,
                 port=RABBITMQ_PORT):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,
                                                                   port=port))
    channel = connection.channel()
    channel.queue_declare(queue=key, durable=True)
    channel.confirm_delivery()

    try:
        channel.basic_publish(exchange="", routing_key=key, body=message,
                              properties=pika.BasicProperties(
                                    content_type="application/json",
                                    delivery_mode=2
                              ), mandatory=True)
    except:
        raise Exception("Failed to send message!")

    connection.close()
