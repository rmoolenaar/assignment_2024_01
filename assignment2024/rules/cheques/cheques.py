"""Cheques rule instance."""
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import assignment2024.constants as c

from ..Rule import Rule


class Cheques(Rule):
    """Cheques rule."""

    def __init__(self, date_from: datetime, date_until: datetime) -> None:
        """Initialize rule instance.

        :param date_from: Start date of monitoring period.
        :param date_until: End date of monitoring period.
        """
        super().__init__(
            "cheques", date_from, date_until, threshold_count=5, transaction_type=c.TRANSACTION_TYPE_CHQ, window_days=20
        )

    def score_rule(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Score the rule.

        :param sdf: Spark dataframe.
        :return: Spark dataframe with alerts.
        """
        # Get window definition
        w = self.define_window()
        # Get transaction count over given period
        sdf = sdf.withColumn("transaction_count", F.count(F.col(c.CHECK_NO_ATTRIBUTE_NAME)).over(w))
        # Filter outcome using minimum threshold value
        sdf = sdf.filter(F.col("transaction_count") >= F.lit(self.threshold_count))

        return sdf
