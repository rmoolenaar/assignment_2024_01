"""Main assignment 2024 module."""
import sys
from typing import Optional

from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.monitor import monitor_date, monitor_today


def monitor(spark: SparkSession, filepath: str, date_str: Optional[str] = None) -> None:
    """Main function.

    :param spark: Spark session.
    :param filepath: path of the transaction file.
    :param date_str: date for monitor for (optional)
    """
    # Load transactions
    sdf = spark.read.csv(filepath, sep=";", header=True, inferSchema=True)

    # Prepare transactions
    sdf = prepare_data(sdf)

    if date_str is None:
        alerts_sdf = monitor_today(sdf)
    else:
        alerts_sdf = monitor_date(sdf, date_str)

    print("Number of alerts:", alerts_sdf.count())


if __name__ == "__main__":
    spark_local = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )
    filestr = sys.argv[1] if len(sys.argv) > 1 else "tests/data/test_transactions.csv"
    datestr = sys.argv[2] if len(sys.argv) > 2 else "01-06-2017" if len(sys.argv) == 1 else None
    monitor(spark_local, filestr, datestr)
