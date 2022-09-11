from Enums import Interval
import DataFetcher

INTERVALS_TO_UPDATE = [Interval.Daily, Interval.Hourly, Interval.Minute_30]
WEEKLY = 7


def update_tables():
    tickers = DataFetcher.get_tickers()  # TODO Store this list in a table
    for ticker in tickers:
        # TODO batch process this anonymously
        for interval in INTERVALS_TO_UPDATE:
            df = DataFetcher.fetch_yahoo_data(ticker, WEEKLY, interval)
            DataFetcher.insert(ticker, interval, df)


if __name__ == "__main__":
    update_tables()
