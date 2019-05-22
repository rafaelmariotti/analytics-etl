"""Pre setup for tests."""
import logging
import pytest
from scripts.spark.utils import create_spark_session


@pytest.fixture(scope="session")
def spark_session(request):
    """Create spark session."""
    with create_spark_session() as spark:
        return spark
