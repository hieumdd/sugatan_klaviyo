from unittest.mock import Mock

from main import main
from .utils import encode_data

def test_broadcast_metrics():
    data = {"broadcast": True, "mode": "metrics"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    for i in res['results']:
        assert i > 0

def test_broadcast_campaigns():
    data = {"broadcast": True, "mode": "campaigns"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    for i in res['results']:
        assert i > 0
