from .utils import process


def test_metrics_manual():
    data = {"mode": "metrics", "start": "2021-06-01", "end": "2021-07-01"}
    process(data)
