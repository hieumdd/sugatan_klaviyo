import os

from .utils import process

CLIENT_NAME = "SBLA"
PRIVATE_KEY = os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY")


def test_metrics():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": PRIVATE_KEY,
        "mode": "metrics",
    }
    process(data)


def test_campaigns():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": PRIVATE_KEY,
        "mode": "campaigns",
    }
    process(data)
