"""Monitor all rules based on given transaction data."""
from datetime import datetime

from pyspark.sql import DataFrame as SparkDataFrame

from .account_balance.account_balance import AccountBalance
from .cheques.cheques import Cheques
from .in_out_behaviour.in_out_behaviour import InOutBehaviour


def monitor_today(sdf: SparkDataFrame) -> SparkDataFrame:
    """Score all rules based on given transaction data

    :param sdf: Spark dataframe with transactions.
    :return: Spark dataframe with alerts
    """
    # Get today
    today = datetime.today()

    # Score rules
    return perform_rule_scoring(sdf, today)


def monitor_date(sdf: SparkDataFrame, date_str: str) -> SparkDataFrame:
    """Score all rules based on given transaction data

    :param sdf: Spark dataframe with transactions.
    :param date_str: date to monitor for (format dd-mm-yyyy)
    :return: Spark dataframe with alerts
    """
    # Get date from string
    date = datetime.strptime(date_str, "%d-%m-%Y")

    # Score rules
    return perform_rule_scoring(sdf, date)


def perform_rule_scoring(sdf: SparkDataFrame, date: datetime) -> SparkDataFrame:
    """Score all rules based on given transaction data

    :param sdf: Spark dataframe with transactions.
    :param date: date to monitor for
    :return: Spark dataframe with alerts
    """
    # Account balance
    ab = AccountBalance(date, date)
    alerts = ab.score(sdf)
    # Cheques
    ch = Cheques(date, date)
    alerts = alerts.union(ch.score(sdf))
    # In-out behaviour
    iob = InOutBehaviour(date, date)
    alerts = alerts.union(iob.score(sdf))

    return alerts
