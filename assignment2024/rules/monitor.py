"""Monitor all rules based on given transaction data."""
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from ..constants import DATE_ATTRIBUTE_NAME
from .account_balance.account_balance import AccountBalance
from .cheques.cheques import Cheques
from .in_out_behaviour.in_out_behaviour import InOutBehaviour


def monitor_today(sdf: SparkDataFrame) -> SparkDataFrame:
    """Score all rules based on given transaction data.

    :param sdf: Spark dataframe with transactions.
    :return: Spark dataframe with alerts
    """
    # Get today
    today = datetime.today()

    # Score rules
    return perform_rule_scoring(sdf, today, today)


def monitor_date(sdf: SparkDataFrame, date_str: str) -> SparkDataFrame:
    """Score all rules based on given transaction data.

    :param sdf: Spark dataframe with transactions.
    :param date_str: date to monitor for (format dd-mm-yyyy)
    :return: Spark dataframe with alerts
    """
    # Get date from string
    date = datetime.strptime(date_str, "%d-%m-%Y")

    # Score rules
    return perform_rule_scoring(sdf, date, date)


def monitor_historical(sdf: SparkDataFrame) -> SparkDataFrame:
    """Score all rules for the whole monitoring period.

    :param sdf: Spark dataframe with transactions.
    :return: Spark dataframe with alerts
    """
    # Get date from string
    start_date = datetime.strptime("01-01-1900", "%d-%m-%Y")
    # Get today
    today = datetime.today()

    # Score rules
    return perform_rule_scoring(sdf, start_date, today)


def overview_per_month(sdf: SparkDataFrame) -> SparkDataFrame:
    """Score all rules for the whole monitoring period and group by month.

    :param sdf: Spark dataframe with transactions.
    :return: Spark dataframe with alerts per month
    """
    # Get all historical alerts
    alerts_sdf = monitor_historical(sdf)

    # Get month + year column as string
    overview_sdf = alerts_sdf.withColumn("year_month", F.date_format(F.col(DATE_ATTRIBUTE_NAME), "yyyy-MM"))

    # Group by year_month
    overview_sdf = overview_sdf.groupBy("year_month").count().orderBy("year_month")

    return overview_sdf


def perform_rule_scoring(sdf: SparkDataFrame, date_from: datetime, date_until: datetime) -> SparkDataFrame:
    """Score all rules based on given transaction data and period.

    :param sdf: Spark dataframe with transactions.
    :param date_from: start date of the monitoring period
    :param date_until: end date of the monitoring period
    :return: Spark dataframe with alerts
    """
    # Account balance
    ab = AccountBalance(date_from, date_until)
    alerts = ab.score(sdf)
    # Cheques
    ch = Cheques(date_from, date_until)
    alerts = alerts.union(ch.score(sdf))
    # In-out behaviour
    iob = InOutBehaviour(date_from, date_until)
    alerts = alerts.union(iob.score(sdf))

    return alerts
