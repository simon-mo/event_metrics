# Example of monitoring performance leak application

import random
import time
from datetime import timedelta
from tempfile import mkstemp

from event_metrics import MetricConnection

_, path = mkstemp()
db = MetricConnection(path)

db.observe("latency", 2.0, labels={"test": "1"})

for i in range(0, 200, 5):
    start = time.time()
    print("sleeping ", i / 100)
    time.sleep(i / 100)
    end = time.time()
    db.observe("latency", end - start)
    print(
        db.query("latency").from_timedelta(timedelta(milliseconds=500)).to_percentiles()
    )
