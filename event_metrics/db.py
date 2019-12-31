import datetime
import itertools
import json
import sqlite3
import tempfile
import time
import warnings
from collections import namedtuple
from typing import Dict, List, Tuple, Union

import numpy as np

from event_metrics.exceptions import MetricNotFound


def _current_time_us():
    return int(time.time() * 1e6)


class MetricConnection:
    """Create or connect to an event_metrics database.

    Args:
        db_path(str, None): The path to database file. If not provided, 
          a temporary file will be created.
    """

    def __init__(self, db_path: Union[str, None] = None):
        if db_path is None:
            _, db_path = tempfile.mkstemp(suffix=".event_metrics.db")

        self._conn = sqlite3.connect(
            db_path, isolation_level=None  # Turn on autocommit mode
        )

        # Turn on write-ahead-logging. In this mode, sqlite3 allows multi-reader
        # single-writer concurrency.
        self._conn.execute("pragma journal_mode=wal")

        # Turn off synchrounous write to the OS. This makes write faster.
        # The risk of intermediate data loss is not too bad because application
        # level crash doesn't impact the right. It will only happen in power down event.
        self._conn.execute("pragma synchronous=0")

        # Create the two tables.
        #   metrics is the source of truth.
        #   cache captures the _last_ items for each time series.
        self._conn.executescript(
            """
-- Table for recording the time series
CREATE TABLE IF NOT EXISTS metrics (
    metric_name TEXT NOT NULL,
    metric_value REAL,
    ingest_time_us INTEGER,
    labels_json TEXT NOT NULL
);

-- Table for accessing latest value
CREATE TABLE IF NOT EXISTS cache (
    metric_name TEXT NOT NULL, 
    metric_value REAL,
    labels_json TEXT NOT NULL,
    PRIMARY KEY (metric_name, labels_json)
);
        """
        )

    def observe(
        self,
        name: str,
        value: Union[float, None] = None,
        *,
        labels: Union[Dict[str, str], None] = None,
        ingest_time_us: Union[int, None] = None,
    ):
        """Record a single metric entry.
        
        Args:
            name(str): Name of the metric, the (name, label) pair should uniquely 
                identify the metric time series.
            value(float, None): The value of the metric. 
            labels(Dict[str,str], None): Key value pair label for the metric, the (name, label) 
                pair should uniquely identify the metric time series.
            ingest_time_us(int, None): Used for testing, if not None, this field overrides the
                default ingestion time, which is the time observe() method is called.
        """
        if ingest_time_us is None:
            ingest_time_us = _current_time_us()

        labels = json.dumps(labels, sort_keys=True)

        self._conn.executescript(
            """
BEGIN TRANSACTION;
INSERT INTO metrics VALUES (:name, :value, :timestamp, :labels_json);
INSERT OR REPLACE INTO cache VALUES (:name, :value, :labels_json);
COMMIT;
            """,
            dict(name=name, value=value, timestamp=ingest_time_us, labels_json=labels),
        )

    def increment(
        self,
        name: str,
        delta: Union[int, float] = 1,
        *,
        labels: Union[Dict[str, str], None] = None,
        ingest_time_us: Union[int, None] = None,
    ):
        """Update value for the time series.

        This method retrieves the latest inserted value for the time series and
        make add a delta value. 

        Example:
            >>> conn.increment("Counter") # defaults to add 1
            >>> conn.increment("Counter", 5) # accepts other values as well
            >>> conn.increment("Connter", -1) # also accepts negative value: decrement

        Args:
            name(str): Time series name.
            delta(int, float): The changes to be made for the latest value.
            labels(dict, None): Time series labels.
            ingest_time_us(int, None): Used for testing, if not None, this field overrides the
                default ingestion time, which is the time increment() method is called.
        """
        if ingest_time_us is None:
            ingest_time_us = _current_time_us()

        labels = json.dumps(labels, sort_keys=True)

        self._conn.executescript(
            """
BEGIN TRANSACTION;
-- If the value doesn't exist, populate the cache
INSERT OR IGNORE INTO cache VALUES (:name, :default_counter, :labels_json); 
UPDATE cache 
    SET metric_value = metric_value + :delta 
    WHERE metric_name = :name and labels_json = :labels_json;

-- Now the cache is populated, we insert the new value into time series table
INSERT INTO metrics VALUES (
    :name, 
    (SELECT metric_value FROM cache WHERE metric_name = :name),
    :timestamp,
    :labels_json);
COMMIT;
            """,
            dict(
                name=name,
                delta=delta,
                timestamp=ingest_time_us,
                labels_json=labels,
                default_counter=0,
            ),
        )

    def query(self, name, *, labels: Union[str, Dict[str, str], None] = "*"):
        """Start a query. To continue, calls from_* and to_* method to compute the final value.
        Event metrics support multi-dimensional querying.

        Example:
            >>> conn.query("latency")
                    .from_timedelta(timedelta(seconds=30))
                    .to_percentiles([50, 90])
        
        Args:
            name(str): Series name.
            labels("*", dict, None): query label.
                - "*": All possible label. Returns a batch.
                - None: Return the default null label. Returns a single query.
                - Dict[str, str]: Perform multi-dimensional label mathcing. Returns a batch.
        """
        cursor = self._conn.execute(
            "SELECT labels_json FROM cache WHERE metric_name = ?", (name,)
        )
        all_labels = itertools.chain.from_iterable(cursor.fetchall())

        if len(all_labels) == 0:
            raise MetricNotFound("Metric {} is not recorded in database.".format(name))

        if isinstance(labels, str) and labels == "*":
            return QueryBatch(
                [Query(self._conn, name, labels=labels) for labels in all_labels]
            )
        elif labels is None:
            if "null" in all_labels:
                return Query(self._conn, name, labels=labels)
            else:
                raise MetricNotFound(
                    "Metric {} doesn't series associated with default label. The labels are {}".format(
                        name, all_labels
                    )
                )
        elif isinstance(labels, dict):
            if "null" in all_labels and len(all_labels) > 0:
                warnings.warn(
                    "Metric {} contains heterogenous labels. It has default labels and other "
                    "key-value pairs. This creates a problem for filtering. The series associated with default "
                    "series is ignored in query. If you are looking for it, use conn.query(..., labels=None). "
                    "All labels associated with the metric are ".format(
                        name, all_labels
                    )
                )

                all_labels.pop("null")

            # Perform multi-dimensional matching
            # Following comments are examples of successful match

            # {"error_code": "404"}
            query_labels = labels

            matched_series_labels = []
            for labels_json in all_labels:
                # {"route": "/index", "error_code": "404"}
                series_labels = json.loads(labels_json)
                # {"error_code"}
                query_keys = set(query_labels.keys())
                # {"error_code"}
                series_key = set(series_labels.keys())
                # {"error_code"}
                key_matches = series_key.intersection(query_keys) == query_keys
                # {"error_code": "404"}
                matched_sub_dict = {
                    k: v for k, v in series_key.items() if k in key_matches
                }
                if matched_sub_dict == query_labels:
                    matched_series_labels.append(series_labels)

            return QueryBatch(
                [
                    Query(self._conn, name, labels=series_labels)
                    for series_labels in matched_series_labels
                ]
            )

        else:
            raise ValueError("labels input can only be '*', dict or None")


class Query:
    def __init__(self, conn, metric_name, labels: Union[Dict[str, str], None] = None):
        self._conn = conn
        self._construction_time = _current_time_us()
        self._metric_name = metric_name
        self._time_cutoff_us = 0

        self.labels = labels
        if self.labels is not None:
            self.labels = json.dumps(self._labels, sort_keys=True)

    def _fetch_array(self, projection_clause):
        stmt = (
            """
SELECT {projection} FROM metrics 
WHERE metric_name = '{name}' AND ingest_time_us > :cutoff AND labels_json = :labels 
        """,
            dict(
                projection=projection_clause,
                name=self._metric_name,
                cutoff=self._time_cutoff_us,
                labels=self._labels,
            ),
        )
        result_cursor = self._conn.execute(stmt)
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
        return ts.astype(float) / 1e6, data

    def to_timestamps(self) -> np.ndarray:
        result = self._fetch_array("ingest_time_us")
        return (
            np.array(result).reshape(-1).astype(float) / 1e6
            if len(result) != 0
            else np.array([])
        )


class QueryBatch:
    def __init__(self, queries: List[Query]):
        self.queries = queries

    def _make_query_result_batch(self, make_result_lambda):
        return [
            {"labels": query.labels, "result": make_result_lambda(query)}
            for query in self.query_labels
        ]

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
        return self._make_query_result_batch(lambda query: queyr.to_timestamps())
