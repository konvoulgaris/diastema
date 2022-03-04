import json

from flask import Blueprint, request

from utils.db import get_db_connection
from utils.message import send_message

data_loading = Blueprint("data-loading", __name__)


@data_loading.route("/", methods=["POST"])
def index():
    data = request.data

    try:
        job = str(json.loads(data)["job-id"])
    except:
        return "Invalid JSON", 400

    try:
        db = get_db_connection()
    except:
        return "Internal server error", 500

    collection = db["Diastema"]["DataLoading"]
    match = collection.find_one({"job-id": job})

    if match:
        db.close()
        return "Job ID already exists", 400

    collection.insert_one({ "job-id": job, "status": "progress", "result": "" })
    db.close()

    try:
        send_message("data-loading", data)
    except:
        return "Failed to submit Data Loading job", 500

    return "Data Loading job submitted", 200


@data_loading.route("/progress", methods=["GET"])
def data_loading_progress():
    job = request.args.get("id")

    if not job:
        return "Missing ID argument", 400

    try:
        db = get_db_connection()
    except:
        return "Internal server error", 500

    collection = db["Diastema"]["DataLoading"]
    match = collection.find_one({"job-id": job})
    db.close()

    if not match:
        return "Job ID doesn't exist", 404

    return match["status"], 200


@data_loading.route("/<job>", methods=["GET"])
def data_loading_job(job):
    try:
        db = get_db_connection()
    except:
        return "Internal server error", 500

    collection = db["Diastema"]["DataLoading"]
    match = collection.find_one({"job-id": job})

    if not match:
        return "Job ID doesn't exist", 404

    return match["result"], 200
