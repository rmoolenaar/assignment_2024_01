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

    # Account balance
    ab = AccountBalance(today, today)
    alerts = ab.score(sdf)

    ch = Cheques(today, today)
    alerts = alerts.union(ch.score(sdf))

    iob = InOutBehaviour(today, today)
    alerts = alerts.union(iob.score(sdf))

    return alerts
