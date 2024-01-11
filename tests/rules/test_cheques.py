from datetime import datetime

from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.cheques.cheques import Cheques


def test_cheques(spark_session: SparkSession) -> None:
    """Test cheques."""
    sdf = spark_session.read.csv("tests/data/test_transactions.csv", sep=";", header=True, inferSchema=True)
    sdf = prepare_data(sdf)

    ch = Cheques(datetime.strptime("01-01-2017", "%d-%m-%Y"), datetime.strptime("01-01-2020", "%d-%m-%Y"))
    sdf = ch.score(sdf)

    assert sdf.count() == 1
