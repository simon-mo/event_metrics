import tempfile
import pytest
from serve_metrics import MetricConnection


@pytest.fixture
def metric_conn():
    _, file_name = tempfile.mkstemp()
    print(file_name)
    yield MetricConnection(file_name)
