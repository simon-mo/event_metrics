import datetime
import json
from typing import Dict, List, Tuple, Union

import numpy as np

from event_metrics.utils import _current_time_us


class Query:
    def __init__(
        self, conn, metric_name, labels: Union[Dict[str, str], None, str] = None
    ):
        self._conn = conn
        self._construction_time = _current_time_us()
        self._metric_name = metric_name
        self._time_cutoff_us = 0

        if isinstance(labels, str):  # already serialized
            self.labels = labels
        else:
            self.labels = json.dumps(labels, sort_keys=True)

    def _fetch_array(self, projection_clause):
        data = dict(
            name=self._metric_name,
            cutoff=self._time_cutoff_us,
            labels=self.labels,
        )
        result_cursor = self._conn.execute(
            """
SELECT {} FROM metrics
WHERE metric_name = :name
AND ingest_time_us > :cutoff
AND labels_json = :labels
ORDER BY ingest_time_us
        """.format(
                projection_clause
            ),
            data,
        )

        result_list = result_cursor.fetchall()
        return result_list

    def from_beginning(self):
        # start from the epoch, this is a noop because the default curoff is 0
        self._time_cutoff_us = 0
        return self

    def from_timestamp(self, timestamp: Union[datetime.datetime, float]):
        assert isinstance(
            timestamp, (datetime.datetime, float)
        ), "Please use datetime.datetime object or floating point to indicate timestamp"

        if isinstance(timestamp, datetime.datetime):
            self._time_cutoff_us = int(timestamp.timestamp() * 1e6)

        if isinstance(timestamp, float):
            self._time_cutoff_us = int(timestamp * 1e6)

        return self

    def from_timedelta(self, delta: datetime.timedelta):
        assert isinstance(delta, datetime.timedelta)
        self._time_cutoff_us = self._construction_time - (delta.total_seconds() * 1e6)
        return self

    def to_scaler(self, agg="last") -> Union[None, float]:
        agg = agg.lower()
        assert agg in {"last", "min", "max", "mean", "count", "sum"}

        if agg == "last":  # Fetch from cache for the latest value
            result = self._conn.execute(
                "SELECT metric_value FROM cache WHERE metric_name = ?",
                (self._metric_name,),
            ).fetchone()
            return result[0] if result is not None else None
        else:
            result = self._fetch_array("{agg}(metric_value)".format(agg=agg))
            return result[0][0] if len(result) != 0 else None

    def to_buckets(self, buckets=[0, 0.5, 1.0, 5.0, 10, 100, np.inf], cumulative=False):
        result = self._fetch_array("metric_value")
        if len(result) == 0:
            return np.array([])

        counts, buckets = np.histogram(result, bins=buckets)
        if cumulative:
            counts = np.cumsum(counts)
        return counts

    def to_percentiles(self, percentiles=[50, 90, 95, 99]):
        result = self._fetch_array("metric_value")
        if len(result) == 0:
            return np.array([])
        return np.percentile(result, percentiles)

    def to_array(self) -> np.ndarray:
        result = self._fetch_array("metric_value")
        return np.array(result).reshape(-1) if len(result) != 0 else np.array([])

    def to_timestamps_array(self) -> Tuple[np.ndarray, np.ndarray]:
        result = self._fetch_array("metric_value, ingest_time_us")
        if len(result) == 0:
            return np.array([]), np.array([])
        data, ts = np.array(result).T
        # Casting is necessary because the value can be "string" due to data can be "None"
        datetime_64 = np.array(
            [
                datetime.datetime.fromtimestamp(timestamp_us / 1e6)
                for timestamp_us in ts
            ],
            dtype="datetime64",
        )
        return datetime_64, data

    def to_timestamps(self) -> np.ndarray:
        result = self._fetch_array("ingest_time_us")
        datetime_64 = np.array(
            [
                datetime.datetime.fromtimestamp(timestamp_us / 1e6)
                for timestamp_us in np.array(result).reshape(-1)
            ],
            dtype="datetime64",
        )
        return datetime_64 if len(datetime_64) != 0 else np.array([])


class QueryBatch:
    def __init__(self, queries: List[Query]):
        self.queries = queries

    def __len__(self):
        return len(self.queries)

    def _make_query_result_batch(self, make_result_lambda):
        return [
            {"labels": query.labels, "result": make_result_lambda(query)}
            for query in self.queries
        ]

    def from_beginning(self):
        self.queries = [query.from_beginning() for query in self.queries]
        return self

    def from_timestamp(self, timestamp: Union[datetime.datetime, float]):
        self.queries = [query.from_timestamp(timestamp) for query in self.queries]
        return self

    def from_timedelta(self, delta: datetime.timedelta):
        self.queries = [query.from_timedelta(delta) for query in self.queries]
        return self

    def to_scaler(self, agg="last"):
        return self._make_query_result_batch(lambda query: query.to_scaler(agg))

    def to_buckets(self, buckets=[0, 0.5, 1.0, 5.0, 10, 100, np.inf], cumulative=False):
        return self._make_query_result_batch(
            lambda query: query.to_buckets(buckets, cumulative)
        )

    def to_percentiles(self, percentiles=[50, 90, 95, 99]):
        return self._make_query_result_batch(
            lambda query: query.to_percentiles(percentiles)
        )

    def to_array(self):
        return self._make_query_result_batch(lambda query: query.to_array())

    def to_timestamps_array(self):
        return self._make_query_result_batch(lambda query: query.to_timestamps_array())

    def to_timestamps(self):
        return self._make_query_result_batch(lambda query: query.to_timestamps())
