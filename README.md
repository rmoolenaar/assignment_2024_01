# Assignment
## Assumptions
I have made the following assumptions:
- As "DATE" and "VALUE DATE" are almost always identical, I will only use the "VALUE DATE" in all calculations.
- As alert handlers would like to see the rule that triggered the alert, I have grouped all output by account number, date and rule ID. This means a single account can have multiple alerts per day, but only for different rules.

## Install
Create virtual environment and then:

    pip install -r requirements.txt
## Run unit tests
You can run some unit tests via pytest:

    pytest tests

## CLI options
Run for a default data set and a default date:

    python main.py monitor

Run for a given dateset and for today:

    python main.py monitor data/transactions.csv

Run for a given dataset and for a given date:

    python main.py monitor data/transactions.csv 01-02-2017

To get a historical overview over the whole period:

    python main.py all

To get an aggregated overview per month:

    python main.py overview
