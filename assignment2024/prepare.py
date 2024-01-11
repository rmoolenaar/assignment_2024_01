"""Prepare input data."""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DoubleType


def prepare_data(sdf: SparkDataFrame) -> SparkDataFrame:
    """Prepare input before scoring the rules.

    :param sdf: Spark dataframe with transaction data.
    :return: Prepared Spark dataframe.
    """
    # Remove last column
    sdf = sdf.drop(".")
    sdf = sdf.withColumnRenamed("CHQ.NO.", "CHEQUE NO")

    # Clean account number attribute
    sdf = _clean_account_no(sdf)

    # Clean/prepare amounts
    sdf = _prepare_amount(sdf, "WITHDRAWAL AMT")
    sdf = _prepare_amount(sdf, "DEPOSIT AMT")
    sdf = _prepare_amount(sdf, "BALANCE AMT")

    # Clean/prepare dates
    sdf = _prepare_date(sdf, "DATE")
    sdf = _prepare_date(sdf, "VALUE DATE")
    return sdf


def _clean_account_no(sdf: SparkDataFrame) -> SparkDataFrame:
    """Clean account number.

    :param sdf: Spark dataframe.
    :return: Cleaned Spark dataframe.
    """
    # Remove quotes
    sdf = sdf.withColumn("Account No", F.regexp_replace(F.col("Account No"), "'", ""))

    # Remove leading/trailing spaces
    sdf = sdf.withColumn("Account No", F.trim(F.col("Account No")))

    return sdf


def _prepare_amount(sdf: SparkDataFrame, attribute: str) -> SparkDataFrame:
    """Prepare amount attributes.

    :param sdf: Spark dataframe.
    :param attribute: attribute column name to be prepared.
    :return: Prepared Spark dataframe.
    """
    # Remove tabs/spaces/etc.
    sdf = sdf.withColumn(attribute, F.regexp_replace(F.col(attribute), r"[\r\t\n ]", ""))

    # Remove dots and replace comma to dot
    sdf = sdf.withColumn(attribute, F.regexp_replace(F.col(attribute), r"\.", ""))
    sdf = sdf.withColumn(attribute, F.regexp_replace(F.col(attribute), ",", "."))

    # Type to double
    sdf = sdf.withColumn(attribute, F.col(attribute).cast(DoubleType()))

    return sdf


def _prepare_date(sdf: SparkDataFrame, attribute: str) -> SparkDataFrame:
    """Prepare date attributes.

    :param sdf: SparkDataFrame
    :param attribute: attribute column name to be prepared.
    :return: Prepared Spark dataframe.
    """
    # Convert string to date using format dd-mon-yy
    sdf = sdf.withColumn(attribute, F.to_date(F.col(attribute), "d-MMM-yy"))

    return sdf
