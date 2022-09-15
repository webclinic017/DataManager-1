from dotenv import load_dotenv
import sqlalchemy
import yfinance as yf
import pandas as pd
import os
import logging
from Enums import Interval
from datetime import date
import datetime
from joblib import Parallel, delayed, parallel_backend

INTERVALS_TO_UPDATE = [Interval.Daily, Interval.Hourly, Interval.Minute_30]
INTRADAY_INTERVALS = [Interval.Minute_30]
NUM_OF_DAYS = 7

load_dotenv()
logging.basicConfig(level=logging.INFO)
DB_CONNECTION = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{os.getenv("DB_USER")}:'
    f'{os.getenv("DB_PW")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}'
)


def parallel_process():
    tickers = get_tickers()
    with parallel_backend("threading", n_jobs=10):
        Parallel()(delayed(update_tables())(ticker) for ticker in tickers)


def update_tables(symbol):
    for interval in INTERVALS_TO_UPDATE:
        df = fetch_yahoo_data(symbol, NUM_OF_DAYS, interval)
        insert(symbol, interval, df)


def get_tickers():
    table_name = "Tickers"
    return pd.read_sql(f"SELECT * FROM {table_name}", con=DB_CONNECTION).Symbol.values


def fetch_yahoo_data(symbol, prev_days, interval):
    try:
        logging.info(
            f"Fetching finance data for symbol {symbol} and interval {interval}"
        )
        if interval in INTRADAY_INTERVALS:
            prev_days = 59
        start_date = (date.today() - datetime.timedelta(prev_days)).strftime("%Y-%m-%d")
        end_date = date.today().strftime("%Y-%m-%d")
        ticker = yf.Ticker(symbol)
        historical_data = ticker.history(
            start=start_date, end=end_date, interval=interval.value
        )
        return historical_data.drop(columns=["Dividends", "Stock Splits"])
    except:
        raise ValueError(f"Unable to fetch finance data for ticker {symbol}")


def insert(symbol, interval, df):
    table_name = f"{symbol}_{interval.value}"
    logging.info(f"Updating data for table {table_name}")
    df.to_sql(con=DB_CONNECTION, name=table_name, if_exists="append")


def lambda_handler(event, handler):
    logging.info(f"Starting lambda function")
    parallel_process()
