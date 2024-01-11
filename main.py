"""Main assignment 2024 module."""
import sys

from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.monitor import monitor_today


def main(spark: SparkSession, filepath: str) -> None:
    """Main function.

    :param spark: Spark session.
    :param filepath: path of the transaction file.
    """
    # Load transactions
    sdf = spark.read.csv(filepath, sep=";", header=True, inferSchema=True)

    # Prepare transactions
    sdf = prepare_data(sdf)

    alerts_sdf = monitor_today(sdf)

    print(alerts_sdf.count())


if __name__ == "__main__":
    spark_local = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )
    main(spark_local, sys.argv[1])
