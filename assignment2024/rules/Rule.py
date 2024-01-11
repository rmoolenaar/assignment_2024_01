"""Rule class definition."""
from abc import abstractmethod
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window, WindowSpec
from pyspark.sql.types import LongType, TimestampType

import assignment2024.constants as c


def days(i: Optional[int]) -> int:
    """Convert days to seconds."""
    return i * 86400 if i is not None else 0


class Rule:
    """Base definition for rules."""

    def __init__(
        self,
        rule_id: str,
        date_from: datetime,
        date_until: datetime,
        threshold_amount: Optional[int] = None,
        threshold_ratio: Optional[float] = None,
        threshold_count: Optional[int] = None,
        window_days: Optional[int] = None,
        transaction_type: Optional[str] = None,
    ) -> None:
        """Rule constructor.

        :param rule_id: The ID of rule.
        :param date_from: The start date of the monitoring period.
        :param date_until: The end date of the monitoring period.
        :param threshold_amount: The threshold on amount level.
        :param threshold_ratio: The threshold on ratio level.
        :param threshold_count: The threshold on count/occurrence level.
        :param window_days: The size of the window to monitor (in days).
        :param transaction_type: The type of transaction which are take into account.
        """
        self.rule_id = rule_id
        self.date_from = date_from
        self.date_until = date_until
        self.threshold_amount = threshold_amount
        self.threshold_ratio = threshold_ratio
        self.threshold_count = threshold_count
        self.window_days = window_days
        self.transaction_type = transaction_type

    def score(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Main method: score a rule on the given Spark dataframe.

        :param sdf: The Spark dataframe.
        :return: Spark dataframe containing the alerts.
        """
        # Filter the input data based on dates and transaction type
        sdf = self._filter(sdf)

        # Score rule
        sdf = self.score_rule(sdf)

        # Filter outcome over the monitoring period
        sdf = sdf.filter(F.col(c.DATE_ATTRIBUTE_NAME).between(self.date_from, self.date_until))
        # Group per account / per day to return a single alert per day per customer
        sdf = sdf.groupBy(c.ACCOUNT_ATTRIBUTE_NAME, c.DATE_ATTRIBUTE_NAME).count()
        # Add rule ID to outcome
        sdf = sdf.withColumn(c.RULE_ID_ATTRIBUTE_NAME, F.lit(self.rule_id))

        return sdf

    @abstractmethod
    def score_rule(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Score a rule."""
        pass

    def _filter(self, sdf: SparkDataFrame) -> SparkDataFrame:
        """Filter the incoming transactions.

        :param sdf: The Spark dataframe with the transactions.
        :return: Spark dataframe.
        """
        # Filter transaction type equals 'CHQ'
        if self.transaction_type == c.TRANSACTION_TYPE_CHQ:
            sdf = sdf.filter(F.col(c.CHECK_NO_ATTRIBUTE_NAME).isNotNull())
        return sdf

    def define_window(self) -> WindowSpec:
        """Specify the window operation for the rules."""
        return (
            Window.partitionBy(F.col(c.ACCOUNT_ATTRIBUTE_NAME))
            .orderBy(F.col(c.DATE_ATTRIBUTE_NAME).cast(TimestampType()).cast(LongType()))
            .rangeBetween(-days(self.window_days), Window.currentRow)
        )
