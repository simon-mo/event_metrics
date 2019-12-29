## Serve Metrics
A simple, embeded, persistent metric library for serving systems.
Comparing to other serving system, this library:
- Write data to sqlite3 database
- Aggregate with the full data by default (no reservior sampling)
- Configurable sliding window
- Interpolation support in histogram
- Metric timestamped by default

Usage:
```python
from serve_metric import MetricConnection
metric = MetricConnection("/tmp/metric.db")
metric.observe("latency", 1.3)
metric.observe("latency", 2.3)
metric.observe("latency", 3.3)

metric.query("latency") # Returns np.array, potentially large!
metric.query("latency", 
    percentiles=[50,90,95], 
    past_seconds=[60,600,3600]
)
```