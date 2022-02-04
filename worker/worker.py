import os
import time
import pika
import json
import uuid
import io
import pandas as pd

from Mongo import Mongo
from MinIO import MinIO
from Metadata import Metadata
from parse import get_max_shrink, parse_data_dict, parse_file
from load import load_data_as_dataframe
from clean import drop_null, clean_string, clean_number

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "0.0.0.0")
MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MINIO_HOST = os.getenv("MINIO_HOST", "0.0.0.0")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

time.sleep(10)

print("Connecting to RabbitMQ server")

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue="data_loading", durable=True)
channel.queue_declare(queue="data_cleaning", durable=True)

print("Now waiting for messages...")


def data_loading_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["DataLoading"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    input_bucket, input_path, output_bucket, output_path, job_id = parse_data_dict(data)

    files = minio.list_objects(input_bucket, recursive=True)
    exports = list()
    metadata = Metadata(f"Dataset", "N/A", output_path, "N/A", 0, 0, 0)

    for f in files:
        file_path, file_directory, file_extension = parse_file(f.object_name)

        if file_directory != input_path:
            continue

        data = minio.get_object(input_bucket, file_path).read()
        data = io.BytesIO(data)
        data.seek(0)

        df = load_data_as_dataframe(data, file_extension)

        if df.empty:
            continue

        df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
        df_data = df.to_csv(index=False).encode("utf-8")
        df_length = len(df_data)
        df_data = io.BytesIO(df_data)
        df_data.seek(0)

        minio.put_object(output_bucket, df_name, df_data, df_length, content_type="application/csv")
        exports.append(f"{output_bucket}/{df_name}")

        metadata.samples += df.shape[0]
        metadata.features += df.shape[1]
        metadata.size += df.memory_usage(deep=True).sum()

    mongo.update_one({"job-id": job_id}, {
        "$set": {
            "status": "complete",
            "result": json.dumps({
                "job-id": job_id,
                "loaded": exports,
                "metadata": metadata.to_dict()
            })
        }
    })

    ch.basic_ack(delivery_tag=method.delivery_tag)


def data_cleaning_callback(ch, method, properties, body):
    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["DataCleaning"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    input_bucket, input_path, output_bucket, output_path, job_id = parse_data_dict(data)
    max_shrink = get_max_shrink(data)

    files = minio.list_objects(input_bucket, recursive=True)
    dfs = list()

    for f in files:
        file_path, file_directory, file_extension = parse_file(f.object_name)

        if file_directory != input_path:
            continue

        data = minio.get_object(input_bucket, file_path).read()
        data = io.BytesIO(data)
        data.seek(0)

        df = load_data_as_dataframe(data, file_extension)
        
        if df.empty:
            continue

        dfs.append(df)

    df = pd.concat(dfs).reset_index(drop=True)
    df = drop_null(df, max_shrink)
    df = clean_string(df)
    df = clean_number(df)

    df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
    df_data = df.to_csv(index=False).encode("utf-8")
    df_length = len(df_data)
    df_data = io.BytesIO(df_data)
    df_data.seek(0)

    minio.put_object(output_bucket, df_name, df_data, df_length, content_type="application/csv")
    
    mongo.update_one({"job-id": job_id}, {"$set": {
        "status": "complete",
        "result": json.dumps({"job-id": job_id})
    }})

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="data_loading", on_message_callback=data_loading_callback)
channel.basic_consume(queue="data_cleaning", on_message_callback=data_cleaning_callback)
channel.start_consuming()
