import numpy as np
from collections import namedtuple
import sqlite3
import time

from typing import List


def _current_time_us():
    return int(time.time() * 1e6)


class MetricConnection:
    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(
            db_path, isolation_level=None  # Turn on autocommit mode
        )

        # Turn on write-ahead-logging. In this mode, Sqlite3 allows multi-reader
        # single-writer concurrency.
        self._conn.execute("pragma journal_mode=wal")

        # Turn off synchrounous write to the OS. This makes write faster.
        # The risk of intermediate data loss is not too bad because application
        # level crash doesn't impact the right. It will only happen in power down event.
        self._conn.execute("pragma synchronous=0")

        self._conn.execute(
            """
CREATE TABLE metrics (
    metric_name INTEGER,
    metric_value REAL,
    ingest_time_us INTEGER
)
        """
        )

    def observe(self, name: str, value: float, *, ingest_time_us=None):
        if ingest_time_us is None:
            ingest_time_us = _current_time_us()
        self._conn.execute(
            "INSERT INTO metrics VALUES (?, ?, ?)", (name, value, ingest_time_us)
        )

    def query(self, name):
        return Query(self._conn, name)


ResultItem = namedtuple("ResultItem", ["result", "percentile", "past_s"])


class Query:
    def __init__(self, conn, metric_name):
        self._conn = conn
        self._construction_time = _current_time_us()

        # Projection
        self._metric_name = metric_name

        # Selection
        self._do_selection = False
        self._ingest_time_cutoff = None
        self._past_windows = None

        # Aggregation
        self._do_aggregation = False
        self._percentiles = None

        # Final result wrangling
        self._do_squeeze = False

    def _fetch_array(self, name, value=False, timestamp=False, add_where_clause=""):
        assert value or timestamp
        projection_clause = []
        if value:
            projection_clause.append("metric_value")
        if timestamp:
            projection_clause.append("ingest_time_us")
        projection_clause = ", ".join(projection_clause)

        if add_where_clause:
            add_where_clause = "AND " + add_where_clause

        stmt = (
            "SELECT {projection} FROM metrics "
            "WHERE metric_name = '{name}' {additional}".format(
                projection=projection_clause, name=name, additional=add_where_clause
            )
        )
        result_cursor = self._conn.execute(stmt)
        result_list = result_cursor.fetchall()
        return np.array(result_list).squeeze()

    def over_past_seconds(self, seconds: List[int]):
        self._past_windows = [val * 1e6 for val in seconds]
        self._ingest_time_cutoff = self._construction_time - max(self._past_windows)
        self._do_selection = True
        return self

    def percentiles(self, percentiles: List[int]):
        self._do_aggregation = True
        self._percentiles = percentiles
        return self

    def squeeze(self):
        self._do_squeeze = True
        return self

    def compute_scaler(self):
        assert self._do_aggregation
        return self._compute()

    def compute_array(self):
        assert not self._do_aggregation
        return self._compute()

    def _compute(self):
        final_result = []
        if self._do_selection:
            timestamped_values = self._fetch_array(
                self._metric_name,
                value=True,
                timestamp=True,
                add_where_clause="ingest_time_us > {}".format(self._ingest_time_cutoff),
            )

            for time_range in self._past_windows:
                cutoff = self._construction_time - time_range
                if len(timestamped_values) == 0:
                    final_result.append(
                        ResultItem(
                            result=np.array([]),
                            percentile=None,
                            past_s=time_range / 1e6,
                        )
                    )
                    continue

                value_column, timestamp_column = timestamped_values.T
                filtered_value = value_column[timestamp_column > cutoff]
                final_result.append(
                    ResultItem(
                        result=filtered_value, percentile=None, past_s=time_range / 1e6
                    )
                )
        else:
            pure_values = self._fetch_array(self._metric_name, value=True)
            final_result.append(
                ResultItem(result=pure_values, percentile=None, past_s=None)
            )

        if self._do_aggregation:
            aggregated_value = []
            for curr_result in final_result:
                perc_values = (
                    np.percentile(curr_result.result, self._percentiles)
                    .flatten()
                    .tolist()
                )
                for perc_key, perc_value in zip(self._percentiles, perc_values):
                    aggregated_value.append(
                        ResultItem(
                            result=perc_key,
                            percentile=perc_value,
                            window=curr_result.window,
                        )
                    )
            final_result = aggregated_value

        if self._do_squeeze:
            final_result = [
                item.result
                for item in final_result
                if item.percentile is None and item.past_s is None
            ]

            if len(final_result) == 1:
                final_result = final_result[0]

        return final_result
