import json
import base64

from models import KlaviyoMetric, KlaviyoCampaigns
from broadcast import broadcast


def metric_factory(start, end):
    """Factory to create metrics

    Args:
        start (str): Date in %Y-%m-%d
        end (str): Date in %Y-%m-%d

    Returns:
        list: List of Metrics Instances
    """

    metrics = [
        ("Received Email", "count"),
        ("Opened Email", "count"),
        ("Clicked Email", "count"),
        ("Placed Order", "count"),
        ("Placed Order", "value"),
        ("Unsubscribed", "count"),
    ]

    metrics = [
        KlaviyoMetric.create(*metric, start=start, end=end) for metric in metrics
    ]
    return metrics


def main(request):
    """API Gateway

    Args:
        request (flask.request): HTTP Request

    Raises:
        NotImplementedError

    Returns:
        dict: Responses in JSON
    """

    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if 'broadcast' in data:
            results = [broadcast(data)]
        else:
            mode = data.get("mode")
            if mode == "metrics":
                metric_jobs = metric_factory(
                    data.get("start"), data.get("end")
                )
                pipelines = "Klaviyo Metrics"
                results = [job.run() for job in metric_jobs]
            elif mode == "campaigns":
                campaigns = KlaviyoCampaigns()
                pipelines = "Klaviyo Campaigns"
                results = [campaigns.run()]
            else:
                raise NotImplementedError

            responses = {"pipelines": pipelines, "results": results}
            print(responses)
            return responses
    else:
        raise NotImplementedError
