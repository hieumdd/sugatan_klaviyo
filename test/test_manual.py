import os
from unittest.mock import Mock

import pytest

from main import main

CLIENT_NAME = "MotivArt"
START = "2021-08-01"
END = "2021-09-01"


@pytest.mark.parametrize(
    [
        "client_name",
        "mode",
        "start",
        "end",
    ],
    [
        (CLIENT_NAME, "metrics", None, None),
        (CLIENT_NAME, "metrics", START, END),
        (CLIENT_NAME, "campaigns", None, None),
    ],
    ids=[
        "metrics_auto",
        "metrics_manual",
        "campaigns",
    ],
)
def test_units(client_name, mode, start, end):
    data = {
        "client_name": client_name,
        "private_key": os.getenv(f"{client_name}_PRIVATE_KEY"),
        "mode": mode,
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]


@pytest.mark.parametrize(
    [
        "mode",
        "start",
        "end",
    ],
    [
        ("metrics", None, None),
        ("metrics", START, END),
        ("campaigns", None, None),
    ],
    ids=[
        "metrics_auto",
        "metrics_manual",
        "campaigns",
    ],
)
def test_tasks(mode, start, end):
    data = {
        "tasks": mode,
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["tasks"] > 0
