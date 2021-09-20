import os

import pytest

from .utils import process

CLIENT_NAME = "MotivArt"
START = "2021-08-01"
END = "2021-08-20"


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
    process(data)
