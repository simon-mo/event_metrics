import datetime
import itertools
import json
import sqlite3
import tempfile
import time
import warnings
from collections import namedtuple
from pprint import pformat
from typing import Dict, List, Tuple, Union

import numpy as np

from event_metrics.exceptions import MetricNotFound
from event_metrics.query import Query, QueryBatch
from event_metrics.utils import _current_time_us


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
        # level crash doesn't impact the integrity. It will only happen in power down event.
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

        self.in_batch_context = False
        self.in_batch_context_entry_count = 0

    def __enter__(self):
        """Enter batch commit context"""
        self.in_batch_context_entry_count += 1
        if not self.in_batch_context:
            self.in_batch_context = True
            self._conn.execute("BEGIN TRANSACTION")

    def __exit__(self, exc_type, exc_value, traceback):
        self.in_batch_context_entry_count -= 1
        if self.in_batch_context:  # The flag is on
            if self.in_batch_context_entry_count == 0:  # we are top level
                self._conn.commit()
                self.in_batch_context = False

    def begin_transaction(self):
        self.__enter__()

    def commit(self):
        self.__exit__(None, None, None)

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
        data = dict(
            name=name, value=value, timestamp=ingest_time_us, labels_json=labels
        )

        with self:
            self._conn.execute(
                "INSERT INTO metrics VALUES (:name, :value, :timestamp, :labels_json)",
                data,
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO cache VALUES (:name, :value, :labels_json)",
                data,
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
        data = dict(
            name=name,
            delta=delta,
            timestamp=ingest_time_us,
            labels_json=labels,
            default_counter=0,
        )

        with self:
            # If the value doesn't exist, populate the cache
            self._conn.execute(
                "INSERT OR IGNORE INTO cache VALUES (:name, :default_counter, :labels_json)",
                data,
            )
            self._conn.execute(
                "UPDATE cache SET metric_value = metric_value + :delta "
                "WHERE metric_name = :name and labels_json = :labels_json",
                data,
            )
            # Now the cache is populated, we insert the new value into time series table
            self._conn.execute(
                """
    INSERT INTO metrics VALUES (
    :name,
    (SELECT metric_value FROM cache WHERE metric_name = :name),
    :timestamp,
    :labels_json);
        """,
                data,
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
        all_labels = set(itertools.chain.from_iterable(cursor.fetchall()))

        # Post processing all_labels
        if len(all_labels) == 0:
            raise MetricNotFound("Metric {} is not recorded in database.".format(name))

        # Begin time series filtering
        if isinstance(labels, str) and labels == "*":
            # If there is only one items, squeeze the batch into single query
            if len(all_labels) == 1:
                return Query(self._conn, name, labels=all_labels.pop())

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
                all_labels.remove("null")

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
                key_matches = series_key.intersection(query_keys)
                if key_matches == query_keys:
                    # {"error_code": "404"}
                    matched_sub_dict = {
                        k: v for k, v in series_labels.items() if k in key_matches
                    }
                    if matched_sub_dict == query_labels:
                        matched_series_labels.append(series_labels)

            if len(matched_series_labels) == 0:
                raise MetricNotFound(
                    "Metric {} with label {} cannot be found. The labels corresponding "
                    "to the name are: \n{}".format(
                        name, query_labels, pformat(all_labels)
                    )
                )
            elif len(matched_series_labels) == 1:
                return Query(self._conn, name, labels=matched_series_labels.pop())
            else:
                return QueryBatch(
                    [
                        Query(self._conn, name, labels=series_labels)
                        for series_labels in matched_series_labels
                    ]
                )

        else:
            raise ValueError("labels input can only be '*', dict or None")
