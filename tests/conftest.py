import tempfile

import pytest

from event_metrics import MetricConnection


@pytest.fixture
def metric_conn():
    _, file_name = tempfile.mkstemp(suffix=".event_metrics.db")
    print("\n[DEBUG] DB file at", file_name)
    yield MetricConnection(file_name)
