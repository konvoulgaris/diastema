import os
import json
import pika

from flask import Flask, request, jsonify
from pymongo import MongoClient

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 5000))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "0.0.0.0")
MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))

app = Flask(__name__)

mongo = MongoClient(MONGO_HOST, MONGO_PORT)

try:
    mongo.server_info()
except:
    print("Failed to create MongoDB connection!")
    exit(1)

db = mongo["Diastema"]


@app.route("/data-loading", methods=["POST"])
def data_loading():
    try:
        job = json.loads(request.data)["job-id"]
    except:
        return "Invalid JSON", 400

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="data_loading", durable=True)
    channel.basic_publish(exchange="", routing_key="data_loading", body=request.data, properties=pika.BasicProperties(delivery_mode=2))
    connection.close()

    match = db["DataLoading"].find_one({"job-id": job})

    if match:
        return "Job ID already exists", 400
    else:
        db["DataLoading"].insert_one({"job-id": job, "status": "progress", "result": ""})
        return f"Data loading for job {job} in progress", 200


@app.route("/data-loading/progress", methods=["GET"])
def data_loading_progress():
    job = request.args.get("id")

    match = db["DataLoading"].find_one({"job-id": job})

    if match:
        return match["status"], 200
    else:
        return "Job ID doesn't exist", 404


@app.route("/data-loading/<job>", methods=["GET"])
def data_loading_id(job):
    match = db["DataLoading"].find_one({"job-id": job})

    if match:
        return json.loads(match["result"]), 200
    else:
        return "Job ID doesn't exist", 404


@app.route("/data-cleaning", methods=["POST"])
def data_cleaning():
    try:
        job = json.loads(request.data)["job-id"]
    except:
        return "Invalid JSON", 400

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="data_cleaning", durable=True)
    channel.basic_publish(exchange="", routing_key="data_cleaning", body=request.data, properties=pika.BasicProperties(delivery_mode=2))
    connection.close()

    match = db["DataCleaning"].find_one({"job-id": job})

    if match:
        return "Job ID already exists", 400
    else:
        db["DataCleaning"].insert_one({"job-id": job, "status": "progress", "result": ""})
        return f"Data cleaning for job {job} in progress", 200


@app.route("/data-cleaning/progress", methods=["GET"])
def data_cleaning_progress():
    job = request.args.get("id")

    match = db["DataCleaning"].find_one({"job-id": job})

    if match:
        return match["status"], 200
    else:
        return "Job ID doesn't exist", 404


@app.route("/data-cleaning/<job>", methods=["GET"])
def data_cleaning_id(job):
    match = db["DataCleaning"].find_one({"job-id": job})

    if match:
        return json.loads(match["result"]), 200
    else:
        return "Job ID doesn't exist", 404


if __name__ == "__main__":
    app.run(HOST, PORT, True, threaded=True)
