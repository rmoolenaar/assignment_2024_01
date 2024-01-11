"""Main assignment 2024 module."""
import sys
from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.monitor import monitor_date, monitor_historical, monitor_today, overview_per_month


def _load_data(spark: SparkSession, filepath: str) -> SparkDataFrame:
    # Load transactions
    sdf = spark.read.csv(filepath, sep=";", header=True, inferSchema=True)

    # Prepare transactions
    sdf = prepare_data(sdf)

    return sdf


def monitor(spark: SparkSession, filepath: str, date_str: Optional[str] = None) -> None:
    """Main function.

    :param spark: Spark session.
    :param filepath: path of the transaction file.
    :param date_str: date for monitor for (optional)
    """
    # Load transactions
    sdf = _load_data(spark, filepath)

    if date_str is None:
        alerts_sdf = monitor_today(sdf)
    else:
        alerts_sdf = monitor_date(sdf, date_str)

    print("Number of alerts:", alerts_sdf.count())


def monitor_all(spark: SparkSession) -> None:
    """Main function.

    :param spark: Spark session.
    """
    # Load transactions
    sdf = _load_data(spark, "data/transactions.csv")

    alerts_sdf = monitor_historical(sdf)

    alerts_sdf.show()

    print("Number of alerts:", alerts_sdf.count())


def aggregated_per_month(spark: SparkSession) -> None:
    """Main function.

    :param spark: Spark session.
    """
    # Load transactions
    sdf = _load_data(spark, "data/transactions.csv")

    overview_sdf = overview_per_month(sdf)

    overview_sdf.show(n=100)


if __name__ == "__main__":
    # Start Spark
    spark_local = SparkSession.builder.master("local[2]").appName("pyspark-local").enableHiveSupport().getOrCreate()

    if len(sys.argv) == 1:
        print("Add type of output: monitor, all or overview.")
        sys.exit(255)

    # Check arguments
    if sys.argv[1] == "monitor":
        filestr = sys.argv[2] if len(sys.argv) > 2 else "tests/data/test_transactions.csv"
        datestr = sys.argv[3] if len(sys.argv) > 3 else "01-06-2017" if len(sys.argv) == 2 else None
        monitor(spark_local, filestr, datestr)

    if sys.argv[1] == "all":
        monitor_all(spark_local)

    if sys.argv[1] == "overview":
        aggregated_per_month(spark_local)
