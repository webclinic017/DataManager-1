from datetime import date
import datetime
import yfinance as yf


DEFAULT_PREV_DAYS = 50
DEFAULT_INTERVAL = "1d"


def fetch_yahoo_data(symbol, prev_days=DEFAULT_PREV_DAYS, interval=DEFAULT_INTERVAL):
    """
    Fetches historical data from Yahoo Finance
    :param symbol: the ticker symbol
    :param prev_days: number of days back for historical data
    :param interval: data interval
    :return: pandas dataframe
    """
    start_date = (date.today() - datetime.timedelta(prev_days)).strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")
    ticker = yf.Ticker(symbol)
    historical_data = ticker.history(start=start_date, end=end_date, interval=interval)
    df = historical_data.drop(columns=["Dividends", "Stock Splits"])
    return df.astype(int)


if __name__ == "__main__":
    print(fetch_yahoo_data("MSFT"))
