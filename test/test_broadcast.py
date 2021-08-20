from unittest.mock import Mock

from .utils import process_broadcast


def test_broadcast_metrics():
    data = {
        "broadcast": "metrics",
    }
    process_broadcast(data)


def test_broadcast_campaigns():
    data = {
        "broadcast": "campaigns",
    }
    process_broadcast(data)
