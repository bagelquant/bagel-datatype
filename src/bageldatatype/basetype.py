"""
Base type for all data

- panel data
- time series data
- cross sectional data

"""

import pandas as pd
from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class Panel():
    """
    Panel data
    
    with double index:
        - symbol
        - date
    """

    data: pd.DataFrame

    def __post_init__(self):
        self._check_index()
    
    def _check_index(self):
        if not isinstance(self.data.index, pd.MultiIndex):
            raise ValueError("Index must be a MultiIndex")
        if self.data.index.names != ["symbol", "date"]:
            raise ValueError("Index must have names 'symbol' and 'date'")
        if not isinstance(self.data.index.get_level_values("date"), pd.DatetimeIndex):
            raise ValueError("Date index must be a DatetimeIndex")

    @property
    def symbols(self) -> list[str]:
        return self.data.index.get_level_values("symbol").unique().tolist()

    @property
    def dates(self) -> list[datetime]:
        return self.data.index.get_level_values("date").unique().tolist()

    def get_time_series(self, symbol: str):
        return TimeSeries(symbol, self.data.loc[symbol])

    def get_cross_section(self, date: datetime):
        return CrossSection(date, self.data.loc[pd.IndexSlice[:, date], :].droplevel(1))

    def __repr__(self):
        return f"Panel data with {len(self.symbols)} symbols and {len(self.dates)} dates"


@dataclass(slots=True, frozen=True)
class TimeSeries():
    """
    Time series data
    
    with single index:
        - date
    """

    symbol: str
    data: pd.Series | pd.DataFrame

    def __post_init__(self):
        self._check_index()
    
    def _check_index(self):
        if not isinstance(self.data.index, pd.DatetimeIndex):
            raise ValueError("Index must be a DatetimeIndex")

    @property
    def dates(self) -> list[datetime]:
        return self.data.index.tolist()  # type: ignore

    def __repr__(self):
        return f"{self.symbol} Time series data with {len(self.dates)} dates"


@dataclass(slots=True, frozen=True)
class CrossSection():
    """
    Cross sectional data
    
    with single index:
        - symbol
    """

    date: datetime
    data: pd.Series | pd.DataFrame


    @property
    def symbols(self) -> list[str]:
        return self.data.index.tolist()  # type: ignore

    def __repr__(self):
        return f"{self.date} Cross sectional data with {len(self.symbols)} symbols"

