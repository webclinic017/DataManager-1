import logging

from Enums import Interval
import DataFetcher

logging.basicConfig(level=logging.INFO)


def lambda_handler(event, handler):
    logging.info(f"Starting lambda function")
    intervals_to_fill = [Interval.Hourly, Interval.Minute_30]
    DataFetcher.persist_sp_data(intervals_to_fill)
