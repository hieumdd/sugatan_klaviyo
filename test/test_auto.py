import os

from .utils import process

CLIENT_NAME = "SBLA"


def test_metrics_auto():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY"),
        "mode": "metrics",
    }
    process(data)


def test_campaigns():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY"),
        "mode": "campaigns",
    }
    process(data)
