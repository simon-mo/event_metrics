import random
import time
from datetime import datetime, timedelta

import numpy as np
import pytest

from event_metrics import MetricConnection
from event_metrics.exceptions import MetricNotFound


def test_increment(metric_conn: MetricConnection):
    metric_conn.increment("counter", 5)
    metric_conn.increment("counter", -3)

    assert metric_conn.query("counter").to_scaler() == 2


def test_observe_null(metric_conn: MetricConnection):
    metric_conn.observe("obs", ingest_time_us=1e6)
    metric_conn.observe("obs", ingest_time_us=2e6)
    assert len(metric_conn.query("obs").to_timestamps()) == 2
    ts = metric_conn.query("obs").to_timestamps().tolist()
    assert isinstance(ts[0], datetime)
    assert ts == [datetime.fromtimestamp(1), datetime.fromtimestamp(2)]


def test_return_scalers(metric_conn: MetricConnection):
    metric_conn.observe("lat", 1.0)
    metric_conn.observe("lat", 2.0)
    query = metric_conn.query("lat")
    assert query.to_scaler(agg="last") == 2.0
    assert query.to_scaler(agg="min") == 1.0


def test_return_buckets(metric_conn: MetricConnection):
    metric_conn.observe("lat", 1.0)
    metric_conn.observe("lat", 2.0)
    metric_conn.observe("lat", 3.0)
    metric_conn.observe("lat", 4.0)

    query = metric_conn.query("lat")

    buckets = query.to_buckets([0.0, 2.5, 4.5])
    assert np.array_equal(buckets, [2, 2])

    bucekts = query.to_buckets([0.0, 2.5, 4.5], cumulative=True)
    assert np.array_equal(bucekts, [2, 4])


def test_return_percentiles(metric_conn: MetricConnection):
    for i in range(0, 101):
        metric_conn.observe("lat", i)
    perc = metric_conn.query("lat").to_percentiles([50, 90, 99])
    assert perc.tolist() == [50, 90, 99]


def test_return_array(metric_conn: MetricConnection):
    metric_conn.observe("lat", 1.0)
    metric_conn.observe("lat", 2.0)
    result = metric_conn.query("lat").to_array()
    assert result.tolist() == [1.0, 2.0]


def test_return_array_random_large(metric_conn: MetricConnection):
    arr = [random.randint(0, 1000) for _ in range(1000)]
    [metric_conn.observe("lat", i) for i in arr]
    result = metric_conn.query("lat").to_array()
    assert result.tolist() == arr


def test_return_timestamps_array(metric_conn: MetricConnection):
    metric_conn.observe("lat", 1.0, ingest_time_us=100)
    metric_conn.observe("lat", 2.0, ingest_time_us=200)
    timestamps, data = metric_conn.query("lat").to_timestamps_array()
    assert data.tolist() == [1.0, 2.0]
    # Timestamps should be returned in seconds
    assert timestamps.tolist() == [
        datetime.fromtimestamp(100 / 1e6),
        datetime.fromtimestamp(200 / 1e6),
    ]


def test_projection(metric_conn: MetricConnection):
    # Ingest in order:
    # Time---1---2---3
    # Value--1---2---3
    metric_conn.observe("lat", 1.0, ingest_time_us=1e6)
    metric_conn.observe("lat", 2.0, ingest_time_us=2e6)
    metric_conn.observe("lat", 3.0, ingest_time_us=3e6)

    query = metric_conn.query("lat")
    # Freeze the current time at the 4th second
    query._construction_time = 4e6

    # Now let's test we are getting values from past 1.5s, 2.5s, 3.5s
    # We should expect:
    # Past------------------------Now
    #     ------------------3--[past 1.5s]
    #     ---------------2--3--[past 2.5s]
    #     ------------1--2--3--[past 3.5s]

    assert len(query.from_beginning().to_array()) == 3
    assert len(query.from_timedelta(timedelta(seconds=0)).to_array()) == 0
    assert len(query.from_timedelta(timedelta(seconds=0.2)).to_array()) == 0
    assert len(query.from_timedelta(timedelta(seconds=1.5)).to_array()) == 1
    assert len(query.from_timedelta(timedelta(seconds=2.5)).to_array()) == 2
    assert len(query.from_timedelta(timedelta(seconds=3.5)).to_array()) == 3

    assert len(query.from_timestamp(datetime.now()).to_array()) == 0
    assert len(query.from_timestamp(time.time()).to_array()) == 0


def test_label_mechanism(metric_conn: MetricConnection):
    metric_conn.observe("latency", 2.0, labels={"route": "/app1", "code": "200"})
    metric_conn.observe("latency", 4.0, labels={"route": "/app2", "code": "200"})
    metric_conn.observe("latency", 6.0, labels={"route": "/app3", "code": "200"})

    # Test that single time series can be extracted
    matched_result = (
        metric_conn.query("latency", labels={"route": "/app1"}).to_array().squeeze()
    )
    assert matched_result == 2.0

    # Test that multiple time series can be extracted
    matched_result = metric_conn.query("latency", labels={"code": "200"}).to_array()
    numbers = set()
    assert isinstance(matched_result, list)
    for series in matched_result:
        assert "route" in series["labels"]
        assert "code" in series["labels"]
        numbers.add(float(series["result"].squeeze()))
    assert numbers == {2.0, 4.0, 6.0}

    # Test that failed matches raises error
    with pytest.raises(MetricNotFound):
        metric_conn.query("latency", labels={"not exist": "fail?"})

    # Test default query batch returns all time series
    assert len(metric_conn.query("latency")) == 3

    # Test null label
    metric_conn.observe("latency", 10.0)
    # It will be observed and included
    assert len(metric_conn.query("latency")) == 4
    # It can be explicitly queried with None
    assert metric_conn.query("latency", labels=None).to_array().squeeze() == 10.0
    # It will be ignored in label matching
    assert len(metric_conn.query("latency", labels={"code": "200"})) == 3


def test_empty_db_query(metric_conn: MetricConnection):
    with pytest.raises(MetricNotFound):
        metric_conn.query("not_found")


def test_batch_op(metric_conn: MetricConnection):
    with metric_conn:
        metric_conn.observe("lat", 1.0)
        metric_conn.observe("lat", 2.0)
    arr = metric_conn.query("lat").from_beginning().to_array()
    assert len(arr) == 2

    metric_conn.begin_transaction()
    metric_conn.observe("lon", 1.0)
    metric_conn.observe("lon", 2.0)
    metric_conn.commit()
    arr = metric_conn.query("lon").from_beginning().to_array()
    assert len(arr) == 2


def test_bench_ingestion(metric_conn: MetricConnection, benchmark):
    benchmark(lambda: metric_conn.observe("lat", 1.0))


@pytest.mark.parametrize("batch_size", (1, 10, 100))
def test_bench_batch_ingest(metric_conn: MetricConnection, benchmark, batch_size):
    def batch_op():
        with metric_conn:
            for _ in range(batch_size):
                metric_conn.observe("lat", 1.0)

    benchmark(batch_op)


@pytest.mark.parametrize("cardinality_before", (0, 1000, 10000, 100000))
def test_labeling_change(metric_conn: MetricConnection, benchmark, cardinality_before):
    val = 0
    with metric_conn:
        for _ in range(cardinality_before):
            metric_conn.increment("lat", 1.0, labels={"key": val})
        val += 1

    def op():
        nonlocal val
        metric_conn.increment("lat", 1.0, labels={"key": val})
        val += 1

    benchmark(op)
