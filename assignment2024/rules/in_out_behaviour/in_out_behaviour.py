"""Deposit/withdrawal rule instance."""
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from ...constants import DEPOSIT_ATTRIBUTE_NAME, WITHDRAWAL_ATTRIBUTE_NAME
from ..Rule import Rule


class InOutBehaviour(Rule):
    """In/out behaviour rule."""

    def __init__(self, date_from: datetime, date_until: datetime) -> None:
        """Initialize rule instance.

        :param date_from: Start date of monitoring period.
        :param date_until: End date of monitoring period.
        """
        super().__init__(
            "in_out_behaviour", date_from, date_until, threshold_amount=100000000, threshold_ratio=0.99, window_days=30
        )

    def score_rule(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Score the rule.

        :param sdf: Spark dataframe.
        :return: Spark dataframe with alerts.
        """
        # Get window definition
        w = self.define_window()
        # Get sum of deposits over given period
        sdf = sdf.withColumn("sum_deposits", F.sum(DEPOSIT_ATTRIBUTE_NAME).over(w))
        # Get sum of withdrawals over given period
        sdf = sdf.withColumn("sum_withdrawals", F.sum(WITHDRAWAL_ATTRIBUTE_NAME).over(w))
        # Calculate ratio between deposits and withdrawals
        sdf = sdf.withColumn("deposits_withdrawals_ratio", F.col("sum_deposits") / F.col("sum_withdrawals"))
        # Calculate sum of all transactions
        sdf = sdf.withColumn("deposits_withdrawals_sum", F.col("sum_deposits") + F.col("sum_withdrawals"))
        # Filter outcome using threshold values
        sdf = sdf.filter(F.col("deposits_withdrawals_ratio") >= F.lit(self.threshold_ratio))
        sdf = sdf.filter(F.col("deposits_withdrawals_sum") >= F.lit(self.threshold_amount))

        return sdf
