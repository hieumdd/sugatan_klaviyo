import os


from .utils import process

CLIENT_NAME = "SBLA"


def test_metrics_manual():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY"),
        "mode": "metrics",
        "start": "2021-06-01",
        "end": "2021-06-30",
    }
    process(data)
