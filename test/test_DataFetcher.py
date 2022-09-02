from unittest import TestCase

from src import DataFetcher

EXPECTED_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]
SYMBOL = "MSFT"


class Test(TestCase):
    def test_fetch_yahoo_data(self):
        df = DataFetcher.fetch_yahoo_data(SYMBOL)
        self.assertEqual(df.columns.tolist(), EXPECTED_COLUMNS)
