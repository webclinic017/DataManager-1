from Enums import Interval
import DataFetcher


def lambda_handler(event, handler):
    intervals_to_fill = [Interval.Hourly, Interval.Minute_30]
    DataFetcher.persist_sp_data(intervals_to_fill)
