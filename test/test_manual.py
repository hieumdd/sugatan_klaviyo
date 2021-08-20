import os


from .utils import process

CLIENT_NAME = "MotivArt"
START = "2021-08-01"
END = "2021-08-05"
PRIVATE_KEY = os.getenv(f"{CLIENT_NAME}_PRIVATE_KEY")


def test_metrics():
    data = {
        "client_name": CLIENT_NAME,
        "private_key": PRIVATE_KEY,
        "mode": "metrics",
        "start": START,
        "end": END,
    }
    process(data)
