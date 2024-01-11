"""Account balance rule instance."""
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from ...constants import BALANCE_ATTRIBUTE_NAME
from ..Rule import Rule


class AccountBalance(Rule):
    """Account balance rule."""

    def __init__(self, date_from: datetime, date_until: datetime) -> None:
        """Initialize rule instance.

        :param date_from: Start date of monitoring period.
        :param date_until: End date of monitoring period.
        """
        super().__init__("account_balance", date_from, date_until, threshold_amount=1000000, window_days=20)

    def score_rule(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Score the rule.

        :param sdf: Spark dataframe.
        :return: Spark dataframe with alerts.
        """
        # Get window definition
        w = self.define_window()
        # Get minimum balance amount over given period
        sdf = sdf.withColumn("min_balance_over_window", F.min(BALANCE_ATTRIBUTE_NAME).over(w))
        # Filter outcome using minimum threshold value
        sdf = sdf.filter(F.col("min_balance_over_window") >= F.lit(self.threshold_amount))

        return sdf
