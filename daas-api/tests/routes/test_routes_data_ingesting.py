import pytest
import mongomock
import json

from flask.testing import FlaskClient

from app.utils.db import get_db_connection


@mongomock.patch(servers=(("0.0.0.0", 27017), ))
def test_routes_data_ingesting_index(test_client: FlaskClient):
    # Test invalid JSON
    res = test_client.post("/data-ingesting", follow_redirects=True,
                           data="Hello")
    assert res.status_code == 400
    assert res.data == b"Invalid JSON"

    # Test already existing Job ID
    data = { "job-id": "1", "minio-input": "path", "minio-output": "path" }
    data_json = json.dumps(data)

    db = get_db_connection()
    collection = db["Diastema"]["DataIngesting"]
    collection.insert_one(data)

    res = test_client.post("/data-ingesting", follow_redirects=True,
                           data=data_json)
    assert res.status_code == 400
    assert res.data == b"Job ID already exists"

    collection.delete_one(data)

    # Test valid
    res = test_client.post("/data-ingesting", follow_redirects=True,
                           data=data_json)
    assert res.status_code == 200
    assert res.data == b"Data Ingesting job submitted"

    match = collection.find_one({ "job-id": data["job-id"] })
    assert match

    db.close()


@mongomock.patch(servers=(("0.0.0.0", 27017), ))
def test_routes_data_ingesting_progress(test_client: FlaskClient):
    # Test missing ID
    res = test_client.get("/data-ingesting/progress", follow_redirects=True)
    assert res.status_code == 400
    assert res.data == b"Missing ID argument"

    # Test invalid ID
    query = { "id": "1" }
    res = test_client.get("/data-ingesting/progress", follow_redirects=True,
                          query_string=query)
    assert res.status_code == 404
    assert res.data == b"Job ID doesn't exist"

    # Test valid
    data = { "job-id": query["id"], "status": "progress", "result": "" }
    db = get_db_connection()
    collection = db["Diastema"]["DataIngesting"]
    collection.insert_one(data)
    db.close()

    res = test_client.get("/data-ingesting/progress", follow_redirects=True,
                          query_string=query)
    assert res.status_code == 200
    assert res.data == b"progress"


@mongomock.patch(servers=(("0.0.0.0", 27017), ))
def test_routes_data_ingesting_job(test_client: FlaskClient):
    # Test missing ID
    job = "1"
    res = test_client.get(f"/data-ingesting/{job}", follow_redirects=True)
    assert res.status_code == 404
    assert res.data == b"Job ID doesn't exist"

    # Test valid
    data = { "job-id": job, "status": "complete", "result": "test" }
    db = get_db_connection()
    collection = db["Diastema"]["DataIngesting"]
    collection.insert_one(data)
    db.close()

    res = test_client.get(f"/data-ingesting/{job}", follow_redirects=True)
    assert res.status_code == 200
    assert res.data == b"test"
