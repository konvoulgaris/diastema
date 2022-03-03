import pytest

from flask.testing import FlaskClient


def test_app_index(test_client: FlaskClient):
    res = test_client.get("/")
    assert res.status_code == 200
    assert res.data == b"Welcome to the Diastema DaaS API!"
