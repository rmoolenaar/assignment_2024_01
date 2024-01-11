"""Configure Spark environment for unit tests."""

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession


def quiet_py4j() -> None:
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request: FixtureRequest) -> SparkSession:
    """Fixture for creating a spark context."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark
