from .utils import process


def test_metrics_auto():
    data = {"mode": "metrics"}
    process(data)


def test_campaigns():
    data = {"mode": "campaigns"}
    process(data)
