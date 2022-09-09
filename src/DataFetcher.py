from datetime import date
import datetime

import sqlalchemy
import yfinance as yf
import pandas as pd
import pathlib
from dotenv import load_dotenv
import mysql.connector as connection
import os
import enum
import logging

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

DEFAULT_PREV_DAYS = 250
DEFAULT_INTERVAL = "1d"

SP_TICKERS = "S&P500Tickers.csv"

CUR_PATH = str(pathlib.Path().resolve())

DB_CONNECTION = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{os.getenv("DB_USER")}:'
    f'{os.getenv("DB_PW")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}'
)


class Interval(enum.Enum):
    Daily = "daily"
    Hourly = "hourly"
    Minute_30 = "minute_30"
    Minute_5 = "minute_5"


def create_database(cursor, db_name):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_name)
        )
    except connection.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


def read_from_table(symbol, interval):
    table_name = f"{symbol}_{interval.value}"
    return pd.read_sql(f"SELECT * FROM {table_name}", con=DB_CONNECTION)


def create_table(symbol, interval, df):
    table_name = f"{symbol}_{interval.value}"
    logging.info(f"Creating table {table_name} and inserting data into table")
    df.to_sql(con=DB_CONNECTION, name=table_name, if_exists="replace")


def insert(symbol, interval, df):
    table_name = f"{symbol}_{interval}"
    df.to_sql(con=DB_CONNECTION, name=table_name, if_exists="append")


def fetch_yahoo_data(symbol, prev_days=DEFAULT_PREV_DAYS, interval=DEFAULT_INTERVAL):
    """
    Fetches historical data from Yahoo Finance
    :param symbol: the ticker symbol
    :param prev_days: number of days back for historical data
    :param interval: data interval
    :return: pandas dataframe
    """
    try:
        logging.info(f"Fetching finance data for symbol {symbol}")
        start_date = (date.today() - datetime.timedelta(prev_days)).strftime("%Y-%m-%d")
        end_date = date.today().strftime("%Y-%m-%d")
        ticker = yf.Ticker(symbol)
        historical_data = ticker.history(
            start=start_date, end=end_date, interval=interval
        )
        return historical_data.drop(columns=["Dividends", "Stock Splits"])

    except:
        raise ValueError(f"Unable to fetch finance data for ticker {ticker}")


def get_sp_tickers():
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    tickers = df.Symbol.values.tolist()
    save_to_csv(pd.DataFrame({"Symbol": tickers}), SP_TICKERS)
    return tickers


def save_to_csv(df, file_name):
    df.to_csv(CUR_PATH + "/resource/" + file_name, index=False)


def load_csv(file_name):
    return pd.read_csv(CUR_PATH + "/resource/" + file_name)


def persist_sp_data():
    tickers = load_csv(SP_TICKERS).Symbol.values
    for ticker in tickers:
        try:
            df = fetch_yahoo_data(ticker)
            create_table(ticker, Interval.Daily, df)
        except:
            print(f"Skipping ticker {ticker}")
            continue


if __name__ == "__main__":
    persist_sp_data()
