from datetime import datetime

from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.in_out_behaviour.in_out_behaviour import InOutBehaviour


def test_in_out_behaviour(spark_session: SparkSession) -> None:
    """Test in-out behaviour"""
    sdf = spark_session.read.csv("tests/data/test_transactions.csv", sep=";", header=True, inferSchema=True)
    sdf = prepare_data(sdf)

    iob = InOutBehaviour(datetime.strptime("01-01-2017", "%d-%m-%Y"), datetime.strptime("16-08-2017", "%d-%m-%Y"))
    sdf = iob.score(sdf)

    assert sdf.count() == 1
