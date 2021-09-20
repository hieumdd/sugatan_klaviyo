import json
import base64

from models import Klaviyo, KlaviyoMetric
from broadcast import broadcast


def main(request):
    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if "broadcast" in data:
            results = [broadcast(data)]
        else:
            mode = data.get("mode")
            if mode == "metrics":
                job = KlaviyoMetric(
                    data["client_name"],
                    data["private_key"],
                    data.get("start"),
                    data.get("end"),
                )
                response = job.run()
            elif mode == "campaigns":
                campaigns = Klaviyo.factory(
                    data["client_name"],
                    data["private_key"],
                    mode,
                )
                results = [campaigns.run()]
            else:
                raise NotImplementedError

        print(response)
        return response
    else:
        raise NotImplementedError
