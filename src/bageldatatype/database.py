"""
Using bageltushare mysql database
"""

import pandas as pd

from dataclasses import dataclass, field
from sqlalchemy import create_engine, Engine, text
from datetime import datetime
from typing import Literal


@dataclass(slots=True, frozen=True)
class BagelMySQL:

    host: str
    port: int
    user: str
    password: str
    database: str


    def get_engine(self) -> Engine:
        return create_engine(
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                )


@dataclass(slots=True, frozen=True)
class Query:

    engine: Engine

    def show_tables(self) -> list[str]:
        with self.engine.begin() as conn:
            result = conn.execute(text("SHOW TABLES"))  
            return [row[0] for row in result]

    def get_trade_cal(self, 
                      is_open: Literal[0, 1, None] = None,
                      market: Literal['cn', 'us'] = 'cn') -> pd.DataFrame:
        if market == 'cn':
            table_name = 'trade_cal'
        elif market == 'us':
            table_name = 'us_tradecal'

        match is_open:
            case 0:
                query: str = 'is_open == 0'
            case 1:
                query: str = 'is_open == 1'
            case None:
                return pd.read_sql_table(table_name, self.engine, index_col='cal_date', parse_dates=['cal_date'])
        return pd.read_sql_table(table_name, self.engine, index_col='cal_date', parse_dates=['cal_date']).query(query)

    def get_stock_basic(self, 
                        codes: list[str] | str | None = None,
                        market: Literal['cn', 'us'] = 'cn') -> pd.DataFrame:
        if market == 'cn':
            table_name = 'stock_basic'
        elif market == 'us':
            table_name = 'us_basic'

        if codes is None:
            return pd.read_sql_table(table_name, self.engine, index_col='ts_code')
        else:
            option: str = f'ts_code in {tuple(codes)}'
            return pd.read_sql_table(table_name, self.engine, index_col='ts_code').query(option)


@dataclass(slots=True, frozen=True)
class StockQuery(Query):
    """
    Query for stock data

    Data with date: (ex. daily)
        will double index, the first index can be either 'code' or 'date'
    Data without date: (ex. stock_basic)
        will have single index, the first index is 'code'

    :param codes: list of stock codes or a single stock code
    :param date_range (optional): tuple of start and end date
    """

    codes: list[str] | str
    date_range: tuple[datetime | None, datetime | None] = field(default=(None, None))
    first_index: Literal['code', 'date'] = 'code'
    market: Literal['cn', 'us'] = 'cn'
    

    def _create_sql(self, table_name: str) -> str:
        """create a sql str based on codes and date_range"""
        if isinstance(self.codes, str):
            sql = f"""
            SELECT * FROM {table_name}
            WHERE ts_code = "{self.codes}"
            """
        else:
            sql = f"""
            SELECT * FROM {table_name}
            WHERE ts_code in {tuple(self.codes)}
            """
        if self.date_range != (None, None):
            start_date, end_date = self.date_range
            sql += f'AND trade_date >= "{start_date}" AND trade_date <= "{end_date}"'
        return sql
 
    def _format_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """set index and sort by date"""
        if self.first_index == 'code':
            return df.set_index(['ts_code', 'trade_date']).sort_index()
        elif self.first_index == 'date':
            return df.set_index(['trade_date', 'ts_code']).sort_index()
        else:
            raise ValueError('first_index must be either "code" or "date"')
       
    def get_daily(self) -> pd.DataFrame:
        if self.market == 'cn':
            table_name = 'daily'
        elif self.market == 'us':
            table_name = 'us_daily'
        else:
            raise ValueError('market must be either "cn" or "us"')
        return pd.read_sql(self._create_sql(table_name),
                           self.engine, 
                           parse_dates=['trade_date']).pipe(self._format_price)

    def get_daily_adj(self) -> pd.DataFrame:
        if self.market == 'cn':
            raise ValueError('adj_daily is not available for cn market')
        elif self.market == 'us':
            table_name = 'us_daily_adj'
        else:
            raise ValueError('market must be either "cn" or "us"')
        return pd.read_sql(self._create_sql(table_name),
                           self.engine, 
                           parse_dates=['trade_date']).pipe(self._format_price)

