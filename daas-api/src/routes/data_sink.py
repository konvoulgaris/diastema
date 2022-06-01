import json

from flask import Blueprint, request

from utils.sink import sink_data

data_sink = Blueprint("data-sink", __name__)


@data_sink.route("/", methods=["POST"])
def index():
    try:
        data = json.loads(request.data)

        if not ("key" in data and "message" in data):
            raise Exception()
    except:
        return "Invalid JSON", 400

    try:
        sink_data(data["key"], data["message"])
    except:
        return "Internal server error", 500

    return "Data sinked", 200
