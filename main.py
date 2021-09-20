import json
import base64

from models import Klaviyo
from tasks import create_task


def main(request):
    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        response = create_task(data)
    elif "client_name" in data:
        job = Klaviyo.factory(
            data["mode"],
            data["client_name"],
            data["private_key"],
            data.get("start"),
            data.get("end"),
        )
        response = job.run()
    else:
        raise ValueError(data)

    print(response)
    return response
