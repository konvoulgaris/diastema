import json

from flask import Blueprint, request

from utils.db import get_db_connection

data_loading = Blueprint("data-loading", __name__)


@data_loading.route("/", methods=["POST"])
def index():
    data = request.data

    try:
        job = str(json.loads(data)["job-id"])
    except:
        return "Invalid JSON", 400

    db = get_db_connection()
    collection = db["Diastema"]["DataLoading"]
    match = collection.find_one({"job-id": job})

    if match:
        db.close()
        return "Job ID already exists", 400

    collection.insert_one({"job-id": job, "status": "progress", "result": ""})

    db.close()

    return f"Data Loading job submitted", 200
