from serve_metrics import __version__, MetricConnection
import numpy as np


def test_version():
    assert __version__ == "0.1.0"


def test_return_array(metric_conn: MetricConnection):
    metric_conn.observe("lat", 1.0)
    metric_conn.observe("lat", 2.0)
    result = metric_conn.query("lat").squeeze().compute_array()
    np.allclose(result, [1.0, 2.0])


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

    result = query.over_past_seconds([1.5, 2.5, 3.5]).compute_array()
    parsed_result = {item.past_s: item.result.tolist() for item in result}
    assert parsed_result == {1.5: [3.0], 2.5: [2.0, 3.0], 3.5: [1.0, 2.0, 3.0]}

    # Now let's try to project for timeframe with empty data
    result = query.over_past_seconds([0.2]).compute_array()
    assert len(result) == 1
    assert len(result[0].result) == 0


def test_ingestion(metric_conn: MetricConnection, benchmark):
    benchmark(lambda: metric_conn.observe("lat", 1.0))
