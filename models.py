import json
from datetime import datetime
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery

NOW = datetime.utcnow()

API_VER = "v1"
BASE_URL = f"https://a.klaviyo.com/api/{API_VER}"
METRIC_ROW_COUNT = 10000
CAMPAIGN_ROW_COUNT = 100

BQ_CLIENT = bigquery.Client()


class Metric(metaclass=ABCMeta):
    @property
    @abstractmethod
    def params_by(self):
        pass

    def __init__(self, metric, measurement):
        self.metric = metric
        self.measurement = measurement
        self.metric_id = None

    def get(self, client_name, session, private_key, start, end):
        with open(f"configs/{client_name}.json", "r") as f:
            metric_mapper = json.load(f)["metric_mapper"]
        self.metric_id = metric_mapper[self.metric]
        return self._get(session, private_key, start, end)

    def _get(self, session, private_key, start, end):
        params = {
            "api_key": private_key,
            "unit": "day",
            "measurement": self.measurement,
            "by": self.params_by,
            "count": METRIC_ROW_COUNT,
        }
        if start and end:
            params["start_date"] = start
            params["end_date"] = end
        with session.get(
            f"{BASE_URL}/metric/{self.metric_id}/export",
            params=params,
        ) as r:
            res = r.json()
        return self._transform(res)

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


class StandardMetric(Metric):
    params_by = "$message"


class ConversionMetric(Metric):
    params_by = "$attributed_message"


class Klaviyo(metaclass=ABCMeta):
    def __init__(self, client_name, private_key):
        self.client_name = client_name
        self.private_key = private_key
        self.dataset = f"{client_name}_Klaviyo"

    @staticmethod
    def factory(mode, client_name, private_key, start, end):
        if mode == "metrics":
            return KlaviyoMetric(client_name, private_key, start, end)
        elif mode == "campaigns":
            return KlaviyoCampaigns(client_name, private_key)
        else:
            raise ValueError(mode)

    @property
    @abstractmethod
    def table(self):
        pass

    @abstractmethod
    def _get(self):
        pass

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{self.dataset}.{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition=self.write_disposition,
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    @abstractmethod
    def _update(self):
        pass

    def run(self):
        rows = self._get()
        response = {
            "table": self.table,
            "num_processed": len(rows),
        }
        if getattr(self, "start", None) and getattr(self, "end", None):
            response["start"] = self.start
            response["end"] = self.end
        if len(rows) > 0:
            response["output_rows"] = self._load(rows)
        return response


class KlaviyoMetric(Klaviyo):
    table = "Metrics"
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

    def __init__(self, client_name, private_key, start, end):
        super().__init__(client_name, private_key)
        self.start, self.end = start, end

    def _get(self):
        metrics = [
            StandardMetric("Received Email", "count"),
            # StandardMetric("Received Email", "unique"),
            # StandardMetric("Opened Email", "count"),
            # StandardMetric("Opened Email", "unique"),
            # StandardMetric("Clicked Email", "count"),
            # StandardMetric("Clicked Email", "unique"),
            # ConversionMetric("Placed Order", "count"),
            # ConversionMetric("Placed Order", "value"),
            # ConversionMetric("Placed Order", "unique"),
            # ConversionMetric("Unsubscribed", "count"),
            ConversionMetric("Unsubscribed", "unique"),
        ]
        with requests.Session() as session:
            rows = [
                metric.get(
                    self.client_name,
                    session,
                    self.private_key,
                    self.start,
                    self.end,
                )
                for metric in metrics
            ]
        return [i for j in rows for i in j]

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
                        PARTITION BY date, metric, metric_id, attributed_message, measurement
                        ORDER BY _batched_at DESC
                    ) AS row_num
                FROM
                    {self.dataset}.{self.table}
            )
        WHERE
            row_num = 1
        """
        BQ_CLIENT.query(query).result()


class KlaviyoCampaigns(Klaviyo):
    table = "_Campaigns"
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
        def get(session, page=0):
            params = {
                "api_key": self.private_key,
                "count": CAMPAIGN_ROW_COUNT,
                "page": page,
            }
            with session.get(
                f"{BASE_URL}/campaigns",
                params=params,
            ) as r:
                res = r.json()
            return res["data"] + get(session, page + 1) if len(res["data"]) > 0 else []

        with requests.Session() as session:
            return get(session)

    def _update(self):
        return super()._update()
