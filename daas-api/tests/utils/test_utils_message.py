import pytest

from app.utils.message import send_message


def test_utils_message_send_message_sucess():
    try:
        send_message("test", "Hello")
    except:
        pytest.fail()


def test_utils_message_send_message_sucess():
    with pytest.raises(Exception):
        send_message("test", "Hello", port=1)

