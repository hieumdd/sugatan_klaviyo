import json
from datetime import datetime
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()

BASE_URL = "https://a.klaviyo.com/api"
API_VER = "v1"

BQ_CLIENT = bigquery.Client()

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


class Klaviyo(metaclass=ABCMeta):
    def __init__(self, client_name, private_key):
        self.client_name = client_name
        self.private_key = private_key
        self.dataset = f"{client_name}_Klaviyo"

    @staticmethod
    def factory(
        client_name,
        private_key,
        mode,
        metric=None,
        measurement=None,
        start=None,
        end=None,
    ):
        if mode == "campaigns":
            return KlaviyoCampaigns(client_name, private_key)
        if mode == "metrics":
            with open(f"configs/{client_name}.json") as f:
                config = json.load(f)
            metric_mapper = config["metric_mapper"]
            args = (
                client_name,
                private_key,
                metric,
                metric_mapper[metric],
                measurement,
                start,
                end,
            )
            if metric in [
                "Unsubscribed",
                "Placed Order",
            ]:
                return KlaviyoConversion(*args)
            elif metric in [
                "Received Email",
                "Opened Email",
                "Clicked Email",
            ]:
                return KlaviyoStandard(*args)
            else:
                raise NotImplementedError(metric)

    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def endpoint(self):
        pass

    @property
    def url(self):
        return f"{BASE_URL}/{API_VER}/{self.endpoint}"

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def transform(self, rows):
        return rows

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{self.dataset}.{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=self.write_disposition,
                schema=self.schema,
            ),
        ).result()

    @abstractmethod
    def update(self):
        pass

    def run(self):
        rows = self.get()
        responses = self.get_responses()
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses = {
                **responses,
                "num_processed": len(rows),
                "output_rows": loads.output_rows,
            }
        return responses

    @abstractmethod
    def get_responses(self, responses):
        pass


class KlaviyoMetric(Klaviyo):
    def __init__(
        self,
        client_name,
        private_key,
        metric,
        metric_id,
        measurement,
        start,
        end,
    ):
        super().__init__(client_name, private_key)
        self.metric = metric
        self.metric_id = metric_id
        self.measurement = measurement
        self.start = start
        self.end = end

    @property
    def table(self):
        return f"_stage_{self.metric}".replace(" ", "")

    @property
    def endpoint(self):
        return f"metric/{self.metric_id}/export"

    @property
    @abstractmethod
    def params_by(self):
        pass

    @property
    def schema(self):
        with open("schemas/metrics.json", "r") as f:
            return json.load(f)

    @property
    def write_disposition(self):
        return "WRITE_APPEND"

    def get(self):
        params = {
            "api_key": self.private_key,
            "unit": "day",
            "measurement": self.measurement,
            "by": self.params_by,
            "count": 10000,
        }
        if self.start and self.end:
            params["start_date"] = self.start
            params["end_date"] = self.end

        with requests.get(self.url, params=params) as r:
            res = r.json()
        return res

    def transform(self, results):
        segments = results["results"]
        rows = [self._parse_segment(segment) for segment in segments]
        rows = [item for sublist in rows for item in sublist]
        rows = [
            {
                "metric": results["metric"]["name"],
                "metric_id": results["metric"]["id"],
                "measurement": self.measurement,
                **row,
                "_batched_at": NOW.isoformat(timespec='seconds'),
            }
            for row in rows
        ]
        return rows

    def _parse_segment(self, segment):
        attributed_message = segment["segment"]
        rows = [
            {
                "attributed_message": attributed_message,
                "date": _data["date"],
                "values": _data["values"][0],
            }
            for _data in segment["data"]
        ]
        return rows

    def update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=self.dataset,
            table=self.table,
            p_key=",".join(
                [
                    "date",
                    "metric",
                    "metric_id",
                    "attributed_message",
                    "measurement",
                ]
            ),
            incremental_key="_batched_at",
        )
        BQ_CLIENT.query(rendered_query)

    def get_responses(self):
        return {
            "metric": self.metric,
            "start_date": getattr(self, "start", None),
            "end_date": getattr(self, "end", None),
        }


class KlaviyoStandard(KlaviyoMetric):
    def __init__(
        self,
        client_name,
        private_key,
        metric,
        metric_id,
        measurement,
        start,
        end,
    ):
        super().__init__(
            client_name,
            private_key,
            metric,
            metric_id,
            measurement,
            start,
            end,
        )

    @property
    def params_by(self):
        return "$message"


class KlaviyoConversion(KlaviyoMetric):
    def __init__(
        self,
        client_name,
        private_key,
        metric,
        metric_id,
        measurement,
        start,
        end,
    ):
        super().__init__(
            client_name,
            private_key,
            metric,
            metric_id,
            measurement,
            start,
            end,
        )

    @property
    def params_by(self):
        return "$attributed_message"


class KlaviyoCampaigns(Klaviyo):
    def __init__(self, client_name, private_key):
        """Initiate Klaviyo Campaigns Job"""

        super().__init__(client_name, private_key)

    @property
    def table(self):
        return "_Campaigns"

    @property
    def endpoint(self):
        return "campaigns"

    @property
    def schema(self):
        with open("schemas/campaigns.json", "r") as f:
            return json.load(f)

    @property
    def write_disposition(self):
        return "WRITE_TRUNCATE"

    def get(self):
        rows = []
        with requests.Session() as session:
            params = {
                "api_key": self.private_key,
                "count": 100,
                "page": 0,
            }
            while True:
                with session.get(self.url, params=params) as r:
                    res = r.json()
                if len(res["data"]) > 0:
                    rows.extend(res["data"])
                    params["page"] += 1
                else:
                    break
        return rows

    def transform(self, rows):
        return rows

    def update(self):
        pass

    def get_responses(self):
        return {
            "table": self.table,
        }
