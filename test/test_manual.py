import os


from .utils import process

CLIENT_NAME = "SBLA"
START = "2021-07-01"
END = "2021-07-28"


def test_metrics_manual():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY"),
        "mode": "metrics",
        "start": START,
        "end": END,
    }
    process(data)
