## Event Metrics

An embedded, event-time metric collection library built for serving system
----------------

Metric systems like prometheus aggregate metric at "processing time", whenever the scraper is 
able to scrape the metric. `event_metrics` capture and record metrics at event time, when the
event happens.

### Features
Comparing to other serving system, the `event_metrics` library:
- Write data to sqlite3 database to keep low-memory footprint
- Minimal dependency (only requires numpy for numeric aggregation)
- Aggregate with the full data by default (no reservior sampling)
- Allow select from past duration with timedelta windowing
- Timestamp all observation by default
- Compute raw timeseries with different aggregation strategies:
    - Scalers: last, min, max, mean, count, sum
    - Buckets for histogram
    - Percentiles for summary
    - Array and imestamps for native python wranging
- Metrics can be labeled with arbitrary key value pair and querying supports 
  multidimensional label matching.

The following features are work in progress
- [ ] Prometheus exporter
- [ ] Altair based plotting dashboard

### Install
- Install from source: `pip install -e .`
- PyPI package is work in progress

### Usage

```python
from event_metrics import MetricConnection, Units

conn = MetricConnection("/tmp/event_metrics_demo")

conn.observe("latency", 1.2)
conn.increment("counter", -1)

# labeling
conn.observe("latency", 2.0, labels={"service": "myapp"})

# querying
(conn.query("latency", labels={"service":"myapp"})
      # select from past duration using one of the following
     .from_beginning()
     .from_timestamp(...)
     .from_timedelta(...)
      
      # perform aggregation using one of the following
     .to_scaler(agg="last/min/max/mean/count/sum") # counter, guage
     .to_buckets(buckets=[1, 5, 10], cumulative=False) # histogram
     .to_percentiles(percnetiles=[50, 90, 99]) # summary
     .to_array() # -> value array
     .to_timestamps() # -> timestamp array
     .to_timestamps_array() # -> 2 array, (timestamp, value array)
)
```

### Speed
The library is *fast enough*. It can ingest about 34,000 data point
per seconds:

You can run `pytest tests -k bench` to generate benchmark on local hardware:
```
-------------------------------------------------- benchmark: 1 tests --------------------------------------------------
Name (time in us)            Min       Max     Mean  StdDev   Median     IQR  Outliers  OPS (Kops/s)  Rounds  Iterations
------------------------------------------------------------------------------------------------------------------------
test_bench_ingestion     25.3340  297.9320  28.8541  9.1582  26.8090  0.8650   521;814       34.6571    6496           1
------------------------------------------------------------------------------------------------------------------------
```