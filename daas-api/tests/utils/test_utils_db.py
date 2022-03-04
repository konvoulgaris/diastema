import pytest
import mongomock

from app.utils.db import get_db_connection


@mongomock.patch(servers=(("0.0.0.0", 27017), ))
def test_db_get_db_connection_success():
    try:
        get_db_connection()
    except:
        pytest.fail()


def test_db_get_db_connection_failure():
    with pytest.raises(Exception):
        get_db_connection(port=1)
