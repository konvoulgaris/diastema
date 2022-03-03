import pytest

from flask import Flask
from flask.testing import FlaskClient

from app import create_app


@pytest.fixture()
def test_app() -> Flask:
    app = create_app()
    yield app


@pytest.fixture()
def test_client(test_app: Flask) -> FlaskClient:
    return test_app.test_client()
