import os
import pika
import json
import uuid
import io
import pandas as pd

from Mongo import Mongo
from MinIO import MinIO
from Metadata import Metadata
from parse import get_max_shrink, parse_data_dict, parse_file
from ingest import download, upload
from load import load_data_as_dataframe
from clean import drop_null, clean_string, clean_number
from join import join


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "0.0.0.0")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MINIO_HOST = os.getenv("MINIO_HOST", "0.0.0.0")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
channel = connection.channel()
channel.queue_declare(queue="data-ingesting", durable=True)
channel.queue_declare(queue="data-loading", durable=True)
channel.queue_declare(queue="data-cleaning", durable=True)
channel.queue_declare(queue="join", durable=True)

print("connected...")

def data_ingesting_callback(ch, method, properties, body):
    print("Data ingesting job received")

    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["DataIngesting"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    print(data)

    url = data.get("url")
    token = data.get("token")
    minio_output = data.get("minio-output")
    job_id = str(data.get("job-id"))
    md = Metadata("Dataset", "N/A", minio_output, "N/A", 0, 0, 0)

    print(f"Data ingesting job {job_id} started")

    try:
        f_name, md = download(url, token)
        upload(f_name, minio, minio_output)
    except Exception as ex:
        print(f"{job_id} data ingesting error")
        print(ex)
        return

    result = json.dumps({
        "job-id": job_id,
        "ingested": minio_output,
        "features": md.features
    })

    match = mongo.find_one({"job-id": job_id})

    mongo.update_one({"_id": match["_id"]}, {
        "$set": {
            "status": "complete",
            "result": result
        }
    })

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"Data ingesting job {job_id} completed")


def data_loading_callback(ch, method, properties, body):
    print(f"Data loading job received")
    
    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["DataLoading"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    input_bucket, input_path, output_bucket, output_path, job_id = parse_data_dict(data)

    files = minio.list_objects(input_bucket, recursive=True)
    exports = list()
    metadata = Metadata([], "Dataset", "N/A", output_path, "N/A", 0, 0, 0)

    print(f"Data loading job {job_id} started")

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
        metadata.features = df.columns
        metadata.size += df.memory_usage(deep=True).sum()

    match = mongo.find_one({"job-id": job_id})

    if match:
        result = json.dumps({
                    "job-id": job_id,
                    "loaded": exports,
                    "metadata": metadata.to_dict()
                })
        mongo.update_one({"_id": match["_id"]}, {
            "$set": {
                "status": "complete",
                "result": result
            }
        })
    else:
        print(f"{job_id} data loading error")
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"Data loading job {job_id} completed")


def data_cleaning_callback(ch, method, properties, body):
    print(f"Data cleaning job received")

    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["DataCleaning"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    input_bucket, input_path, output_bucket, output_path, job_id = parse_data_dict(data)
    max_shrink = get_max_shrink(data)

    files = minio.list_objects(input_bucket, recursive=True)
    dfs = list()

    print(f"Data cleaning job {job_id} started")

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

    match = mongo.find_one({"job-id": job_id})

    if match:
        result = json.dumps({"job-id": job_id})
        mongo.update_one({"job-id": job_id}, {"$set": {
            "status": "complete",
            "result": result
        }})

    else:
        print(f"{job_id} data cleaning error")
        return

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"Data cleaning job {job_id} started")


def join_callback(ch, method, properties, body):
    print("Join job received")

    data = json.loads(body.decode())
    mongo = Mongo(MONGO_HOST, MONGO_PORT)["Diastema"]["Join"]
    minio = MinIO(MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASS)

    print(data)

    column = data.get("column")
    join_type = data.get("type")
    inputs = data.get("inputs")
    output = data.get("output")
    job_id = str(data.get("job-id"))

    print(f"Join job {job_id} started")

    try:
        join(minio, inputs, column, join_type, output)
    except Exception as ex:
        print(f"{job_id} join error")
        print(ex)
        return

    result = json.dumps({
        "job-id": job_id,
        "output": output,
    })

    match = mongo.find_one({"job-id": job_id})

    mongo.update_one({"_id": match["_id"]}, {
        "$set": {
            "status": "complete",
            "result": result
        }
    })

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(f"Join job {job_id} completed")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="data-ingesting", on_message_callback=data_ingesting_callback)
channel.basic_consume(queue="data-loading", on_message_callback=data_loading_callback)
channel.basic_consume(queue="data-cleaning", on_message_callback=data_cleaning_callback)
channel.basic_consume(queue="join", on_message_callback=join_callback)
channel.start_consuming()
