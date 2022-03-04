import pytest
import mongomock
import json

from flask.testing import FlaskClient

from app.utils.db import get_db_connection


@mongomock.patch(servers=(("0.0.0.0", 27017), ))
def test_routes_data_loading_index(test_client: FlaskClient):
    # Test invalid JSON
    res = test_client.post("/data-loading", follow_redirects=True, data="Hello")
    assert res.status_code == 400
    assert res.data == b"Invalid JSON"

    # Test already existing Job ID
    data = { "job-id": "1", "minio-input": "path", "minio-output": "path" }
    data_json = json.dumps(data)

    db = get_db_connection()
    collection = db["Diastema"]["DataLoading"]
    collection.insert_one(data)

    res = test_client.post("/data-loading", follow_redirects=True,
                           data=data_json)
    assert res.status_code == 400
    assert res.data == b"Job ID already exists"

    collection.delete_one(data)

    # Test valid
    res = test_client.post("/data-loading", follow_redirects=True,
                           data=data_json)
    assert res.status_code == 200
    assert res.data == b"Data Loading job submitted"

    match = collection.find_one({ "job-id": data["job-id"] })
    assert match

    db.close()
