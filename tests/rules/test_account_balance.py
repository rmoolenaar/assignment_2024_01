from datetime import datetime

from pyspark.sql import SparkSession

from assignment2024.prepare import prepare_data
from assignment2024.rules.account_balance.account_balance import AccountBalance


def test_account_balance(spark_session: SparkSession) -> None:
    """Test account balance."""
    sdf = spark_session.read.csv("tests/data/test_transactions.csv", sep=";", header=True, inferSchema=True)
    sdf = prepare_data(sdf)

    ab = AccountBalance(datetime.strptime("01-01-2017", "%d-%m-%Y"), datetime.strptime("16-08-2017", "%d-%m-%Y"))
    sdf = ab.score(sdf)

    assert sdf.count() == 2
