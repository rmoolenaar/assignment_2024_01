from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, IntegerType

import assignment2024.constants as c
from assignment2024.prepare import prepare_data


def test_prepare(spark_session: SparkSession) -> None:
    """Test prepare methods"""
    sdf = spark_session.read.csv("tests/data/test_transactions.csv", sep=";", header=True, inferSchema=True)
    sdf = prepare_data(sdf)

    found = False

    for attr in sdf.schema:
        if attr.name == c.DATE_ATTRIBUTE_NAME:
            found = True
            assert attr.dataType == DateType()
        if attr.name == c.BALANCE_ATTRIBUTE_NAME:
            found = True
            assert attr.dataType == DoubleType()
        if attr.name == c.DEPOSIT_ATTRIBUTE_NAME:
            found = True
            assert attr.dataType == DoubleType()
        if attr.name == c.WITHDRAWAL_ATTRIBUTE_NAME:
            found = True
            assert attr.dataType == DoubleType()
        if attr.name == c.CHECK_NO_ATTRIBUTE_NAME:
            found = True
            assert attr.dataType == IntegerType()

    assert found

    assert sdf.count() == 22
