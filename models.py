import json
from datetime import datetime
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()

API_VER = "v1"
BASE_URL = f"https://a.klaviyo.com/api/{API_VER}"
MAX_COUNT = 10000

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
        metric,
        measurement,
        start,
        end,
    ):
        if mode == "campaigns":
            return KlaviyoCampaigns(client_name, private_key)
        if mode == "metrics":
            with open(f"configs/{client_name}.json") as f:
                metric_mapper = json.load(f)["metric_mapper"]
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
                raise ValueError(metric)

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
        return f"{BASE_URL}/{self.endpoint}"

    @abstractmethod
    def _get(self):
        pass

    @abstractmethod
    def _transform(self, rows):
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
    def _update(self):
        pass

    def run(self):
        rows = self._get()
        responses = self.get_responses()
        if len(rows) > 0:
            rows = self._transform(rows)
            loads = self.load(rows)
            self._update()
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
    schema = [
        {"name": "metric", "type": "STRING"},
        {"name": "metric_id", "type": "STRING"},
        {"name": "attributed_message", "type": "STRING"},
        {"name": "date", "type": "TIMESTAMP"},
        {"name": "measurement", "type": "STRING"},
        {"name": "values", "type": "FLOAT"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]
    write_disposition = "WRITE_APPEND"

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

    def _get(self):
        params = {
            "api_key": self.private_key,
            "unit": "day",
            "measurement": self.measurement,
            "by": self.params_by,
            "count": MAX_COUNT,
        }
        if self.start and self.end:
            params["start_date"] = self.start
            params["end_date"] = self.end

        with requests.get(self.url, params=params) as r:
            res = r.json()
        return res

    def _transform(self, results):
        transform_segment = lambda segment: [
            {
                "attributed_message": segment["segment"],
                "date": data["date"],
                "values": data["values"][0],
            }
            for data in segment["data"]
        ]
        rows = [transform_segment(segment) for segment in results["results"]]
        rows = [item for sublist in rows for item in sublist]
        return [
            {
                "metric": results["metric"]["name"],
                "metric_id": results["metric"]["id"],
                "measurement": self.measurement,
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE {self.dataset}.{self.table} AS
        SELECT
            *
        EXCEPT
            (row_num)
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() over (
                        PARTITION BY date,metric,metric_id,attributed_message,measurement,
                        ORDER BY _batched_at DESC
                    ) AS row_num
                FROM
                    {self.dataset}._stage_{self.table}
            )
        WHERE
            row_num = 1
        """
        BQ_CLIENT.query(query)

    def get_responses(self):
        return {
            "metric": self.metric,
            "start_date": getattr(self, "start", None),
            "end_date": getattr(self, "end", None),
        }


class KlaviyoStandard(KlaviyoMetric):
    params_by = "$message"


class KlaviyoConversion(KlaviyoMetric):
    params_by = "$attributed_message"


class KlaviyoCampaigns(Klaviyo):
    table = "_Campaigns"
    endpoint = "campaigns"
    schema = [
        {"name": "object", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "subject", "type": "STRING"},
        {"name": "from_email", "type": "STRING"},
        {"name": "from_name", "type": "STRING"},
        {
            "name": "lists",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "object", "type": "STRING"},
                {"name": "id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "list_type", "type": "STRING"},
                {"name": "folder", "type": "STRING"},
                {"name": "created", "type": "TIMESTAMP"},
                {"name": "updated", "type": "TIMESTAMP"},
                {"name": "person_count", "type": "INTEGER"},
            ],
        },
        {
            "name": "excluded_lists",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "object", "type": "STRING"},
                {"name": "id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "list_type", "type": "STRING"},
                {"name": "folder", "type": "STRING"},
                {"name": "created", "type": "TIMESTAMP"},
                {"name": "updated", "type": "TIMESTAMP"},
                {"name": "person_count", "type": "INTEGER"},
            ],
        },
        {"name": "status", "type": "STRING"},
        {"name": "status_id", "type": "INTEGER"},
        {"name": "status_label", "type": "STRING"},
        {"name": "sent_at", "type": "TIMESTAMP"},
        {"name": "send_time", "type": "TIMESTAMP"},
        {"name": "created", "type": "TIMESTAMP"},
        {"name": "updated", "type": "TIMESTAMP"},
        {"name": "num_recipients", "type": "INTEGER"},
        {"name": "campaign_type", "type": "STRING"},
        {"name": "is_segmented", "type": "BOOLEAN"},
        {"name": "message_type", "type": "STRING"},
        {"name": "template_id", "type": "STRING"},
    ]
    write_disposition = "WRITE_TRUNCATE"

    def _get(self):
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

    def __get(self, session, page=0):
        params = {
            "api_key": self.private_key,
            "count": 100,
            "page": page,
        }
        with session.get(self.url, params=params) as r:
            res = r.json()
        if len(res["data"] > 0):
            return res["data"].extend(self.__get(session, page + 1))

    def _transform(self, rows):
        return rows

    def _update(self):
        pass

    def get_responses(self):
        return {
            "table": self.table,
        }
