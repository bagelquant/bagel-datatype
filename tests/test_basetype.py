import unittest
import pandas as pd
from unittest import TestCase
from datetime import datetime

from src.bageldatatype.basetype import Panel, TimeSeries, CrossSection


class TestBase(TestCase):

    def setUp(self) -> None:
        data = pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL", "AAPL"],
                "date": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"],
                "close": [100, 101, 102, 103, 104, 105], 
                "open": [99, 100, 101, 102, 103, 104]
                }
        )
        data_2 = pd.DataFrame(
            {
                "symbol": ["TSLA", "TSLA", "TSLA", "TSLA", "TSLA", "TSLA"],
                "date": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05", "2021-01-06"],
                "close": [100, 101, 102, 103, 104, 105],
                "open": [99, 100, 101, 102, 103, 104]
            }
        )
        self.data = pd.concat([data, data_2])
        self.data["date"] = pd.to_datetime(self.data["date"])
        self.data.set_index(["symbol", "date"], inplace=True)

    def test_panel(self):
        panel = Panel(self.data)
        self.assertEqual(len(panel.symbols), 2)
        self.assertEqual(len(panel.dates), 6)
        self.assertEqual(panel.symbols, ["AAPL", "TSLA"])
        self.assertEqual(panel.dates, [datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-02", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-03", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-04", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-05", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-06", "%Y-%m-%d")])
        print(panel, panel.data.head())

    def test_timeseries(self):
        panel = Panel(self.data)
        ts = panel.get_time_series("AAPL")
        self.assertEqual(ts.symbol, "AAPL")
        self.assertEqual(len(ts.dates), 6)
        self.assertEqual(ts.dates, [datetime.strptime("2021-01-01", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-02", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-03", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-04", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-05", "%Y-%m-%d"),
                                      datetime.strptime("2021-01-06", "%Y-%m-%d")])
        print(ts, ts.data.head())

    
    def test_crosssection(self):
        panel = Panel(self.data)
        cs = panel.get_cross_section(datetime.strptime("2021-01-01", "%Y-%m-%d"))
        self.assertEqual(cs.date, datetime.strptime("2021-01-01", "%Y-%m-%d"))
        self.assertEqual(len(cs.symbols), 2)
        self.assertEqual(cs.symbols, ["AAPL", "TSLA"])
        print(cs, cs.data.head())


if __name__ == "__main__":
    unittest.main()
