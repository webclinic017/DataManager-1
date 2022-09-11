import enum


class Interval(enum.Enum):
    Daily = "1d"
    Hourly = "1h"
    Minute_30 = "30m"
    Minute_15 = "15m"
    Minute_5 = "5m"
