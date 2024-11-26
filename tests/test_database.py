"""
database module test
"""

import unittest
import json
import pandas as pd

from pathlib import Path
from sqlalchemy import Engine, engine_from_config
from datetime import datetime
from src.bageldatatype.database import BagelMySQL, Query, StockQuery


class TestBagelMySQL(unittest.TestCase):

    def setUp(self) -> None:
        with open(Path(__file__).parent.parent / 'db_config.json') as f:
            db_config = json.load(f)

        self.bagel_mysql = BagelMySQL(**db_config)

    def test_get_engine(self):
        self.assertIsInstance(self.bagel_mysql.get_engine(), Engine)


class TestQuery(unittest.TestCase):

    def setUp(self) -> None:
        with open(Path(__file__).parent.parent / 'db_config.json') as f:
            db_config = json.load(f)

        self.bagel_mysql = BagelMySQL(**db_config)
        self.query = Query(self.bagel_mysql.get_engine())

    def test_show_tables(self):
        self.assertIsInstance(self.query.show_tables(), list)
    
    # trade_cal table
    def test_get_trade_cal(self):
        trade_cal = self.query.get_trade_cal()
        print(f'\ntrade_cal: \n{trade_cal.head(2)}')
        self.assertIsInstance(trade_cal, pd.DataFrame)

    def test_get_trade_cal_is_open(self):
        trade_cal = self.query.get_trade_cal(is_open=1)
        print(f'\ntrade_cal_is_open: \n{trade_cal.head(2)}')
        self.assertIsInstance(trade_cal, pd.DataFrame)
        self.assertTrue(trade_cal['is_open'].all(), 1)

    def test_get_trade_cal_us(self):
        trade_cal = self.query.get_trade_cal(market='us')
        print(f'\ntrade_cal_us: \n{trade_cal.head(2)}')
        self.assertIsInstance(trade_cal, pd.DataFrame)

    def test_get_trade_cal_us_is_open(self):
        trade_cal = self.query.get_trade_cal(is_open=1, market='us')
        print(f'\ntrade_cal_us_is_open: \n{trade_cal.head(2)}')
        self.assertIsInstance(trade_cal, pd.DataFrame)
        self.assertTrue(trade_cal['is_open'].all(), 1)
    
    # stock_basic table
    def test_get_stock_basic(self):
        stock_basic = self.query.get_stock_basic()
        print(f'\nstock_basic: \n{stock_basic.head(2)}')
        self.assertIsInstance(stock_basic, pd.DataFrame)

    def test_get_stock_basic_codes(self):
        stock_basic = self.query.get_stock_basic(codes=['000001.SZ', '000002.SZ'])
        print(f'\nstock_basic_codes: \n{stock_basic.head(2)}')
        self.assertIsInstance(stock_basic, pd.DataFrame)
        self.assertTrue(stock_basic.index.isin(['000001.SZ', '000002.SZ']).all())

    def test_get_stock_basic_us(self):
        stock_basic = self.query.get_stock_basic(market='us')
        print(f'\nstock_basic_us: \n{stock_basic.head(2)}')
        self.assertIsInstance(stock_basic, pd.DataFrame)

    def test_get_stock_basic_us_codes(self):
        stock_basic = self.query.get_stock_basic(codes=['AAPL', 'MSFT'], market='us')
        print(f'\nstock_basic_us_codes: \n{stock_basic.head(2)}')
        self.assertIsInstance(stock_basic, pd.DataFrame)
        self.assertTrue(stock_basic.index.isin(['AAPL', 'MSFT']).all())


class TestStockQuery(unittest.TestCase):

    def setUp(self) -> None:
        with open(Path(__file__).parent.parent / 'db_config.json') as f:
            db_config = json.load(f)

        self.bagel_mysql = BagelMySQL(**db_config)
        self.engine = self.bagel_mysql.get_engine()
        self.date_range = (datetime(2021, 1, 1), datetime(2021, 1, 31))
    #
    # def test_cn_stock_daily_single_without_date(self):
    #     stock_query = StockQuery(engine=self.engine,
    #                              market='cn',
    #                              codes='000001.SZ')
    #     stock_daily = stock_query.get_daily()
    #     print(f'\ncn_stock_daily_single_without_date: \n{stock_daily.head(2)}')
    #     self.assertIsInstance(stock_daily, pd.DataFrame)
    #
    # def test_cn_stock_daily_single_with_date(self):
    #     stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
    #                              market='cn',
    #                              codes='000001.SZ',
    #                              date_range=self.date_range)
    #     stock_daily = stock_query.get_daily()
    #     print(f'\ncn_stock_daily_single_with_date: \n{stock_daily.head(2)}')
    #     self.assertIsInstance(stock_daily, pd.DataFrame)
    #
    # def test_cn_stock_daily_multi_without_date(self):
    #     stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
    #                              market='cn',
    #                              codes=['000001.SZ', '000002.SZ'])
    #     stock_daily = stock_query.get_daily()
    #     print(f'\ncn_stock_daily_multi_without_date: \n{stock_daily.head(2)}')
    #     self.assertIsInstance(stock_daily, pd.DataFrame)
    #
    # def test_cn_stock_daily_multi_with_date(self):
    #     stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
    #                              market='cn',
    #                              codes=['000001.SZ', '000002.SZ'],
    #                              date_range=self.date_range)
    #     stock_daily = stock_query.get_daily()
    #     print(f'\ncn_stock_daily_multi_with_date: \n{stock_daily.head(2)}')
    #     self.assertIsInstance(stock_daily, pd.DataFrame)

    def test_us_stock_daily_single_without_date(self):
        stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
                                 market='us',
                                 codes='AAPL')
        stock_daily = stock_query.get_daily()
        print(f'\nus_stock_daily_single_without_date: \n{stock_daily.head(2)}')
        self.assertIsInstance(stock_daily, pd.DataFrame)

    def test_us_stock_daily_single_with_date(self):
        stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
                                 market='us',
                                 codes='AAPL',
                                 date_range=self.date_range)
        stock_daily = stock_query.get_daily()
        print(f'\nus_stock_daily_single_with_date: \n{stock_daily.head(2)}')
        self.assertIsInstance(stock_daily, pd.DataFrame)

    def test_us_stock_daily_multi_without_date(self):
        stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
                                 market='us',
                                 codes=['AAPL', 'MSFT'])
        stock_daily = stock_query.get_daily()
        print(f'\nus_stock_daily_multi_without_date: \n{stock_daily.head(2)}')
        self.assertIsInstance(stock_daily, pd.DataFrame)

    def test_us_stock_daily_multi_with_date(self):
        stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
                                 market='us',
                                 codes=['AAPL', 'MSFT'],
                                 date_range=self.date_range)
        stock_daily = stock_query.get_daily()
        print(f'\nus_stock_daily_multi_with_date: \n{stock_daily.head(2)}')
        self.assertIsInstance(stock_daily, pd.DataFrame)


    def test_us_stock_daily_multi_date_first_index(self):
        stock_query = StockQuery(engine=self.bagel_mysql.get_engine(),
                                 market='us',
                                 codes=['AAPL', 'MSFT'],
                                 date_range=self.date_range,
                                 first_index='date')
        stock_daily = stock_query.get_daily()
        print(f'\nus_stock_daily_multi_date_first_index: \n{stock_daily.head(2)}')
        self.assertIsInstance(stock_daily, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
