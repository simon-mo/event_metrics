## Event Metrics

Metric systems like prometheus aggregate metric at "processing time", the time
series is captured whenever the scraper is able to scrape the metric. Serve
metric capture metrics at event time.

A simple, embeded, persistent metric library for serving systems.
Comparing to other serving system, this library:
- Write data to sqlite3 database
- Capture data at **event time**
- Aggregate with the full data by default (no reservior sampling)
- Configurable sliding window
- Interpolation support in histogram
- Metric timestamped by default

Usage:
```python
from event_metrics import MetricConnection, Units

conn = MetricConnection("/tmp")

# time series -> counter, guage, histogram, summary, time series
conn.observe("name", 1.2)
conn.increment("counter", -1)

# querying
(conn.query("name")
      
	  # select from past duration
     .from_beginning()
	 .from_timestamp()
	 .from_timedeltas()
	  
	  # perform aggregation
	 .to_scaler(agg="last/min/max/mean/count/sum") # counter, guage
	 .to_buckets(buckets=[1, 5, 10], cumulative=False) # histogram
	 .to_percentiles(percnetiles=[50, 90, 99]) # summary
	 .to_array() # -> value array
	 .to_timestamps() # -> timestamp array
	 .to_timestamps_array() # -> 2 array, (timestamp, value array)
)
```