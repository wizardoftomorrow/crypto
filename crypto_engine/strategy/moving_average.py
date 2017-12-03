import pandas as pd
from crypto_engine.strategy.strategy import Strategy, BUY, SELL
from crypto_engine.common import MARKET, TRADING_SIGNAL

SHORT_WINDOW = 500
LONG_WINDOW = 1000

SHORT_MOV_AVG = 'SHORT_MOVING_AVERAGE'
LONG_MOV_AVG = 'LONG_MOVING_AVERAGE'


class MovingAverage(Strategy):
    """
    A basic Moving Average Crossover strategy that generates
    two simple moving averages (SMA), with default windows

    The strategy is "long only" in the sense it will only
    open a long position once the short SMA exceeds the long
    SMA. It will close the position (by taking a corresponding
    sell order) when the long SMA recrosses the short SMA.
    The strategy uses a rolling SMA calculation in order to
    increase efficiency.
    """

    def __init__(self, short_window=SHORT_WINDOW, long_window=LONG_WINDOW):
        self.short_window = short_window
        self.long_window = long_window

    def buy_indicator(self):
        """
        has no functionality
        """
        return BUY

    def sell_indicator(self):
        """
        has no functionality
        """
        return SELL

    @staticmethod
    def buy_sell_indicator(data):
        if all(data):
            if data[0] > data[1]:
                return BUY
            elif data[1] > data[0]:
                return SELL

    def calculate_signals(self, data, column):
        """
        Arguments:
            data - DataFrame containing necessary data for analysis
            column - column containing data for analysis; it is used to make strategy more flexible performing
                     moving_average on different data formats (ticker, candle, ...)
        """
        markets_with_signal = []
        signals = []
        markets = data[MARKET].unique()
        for market in markets:
            data_market = data[data[MARKET] == market].reset_index()
            data_market[SHORT_MOV_AVG] = data_market[column].rolling(window=SHORT_WINDOW, min_periods=1).mean()
            data_market[LONG_MOV_AVG] = data_market[column].rolling(window=LONG_WINDOW, min_periods=1).mean()

            # only last value is of interest
            signal = self.buy_sell_indicator(data_market.iloc[-1][[SHORT_MOV_AVG, LONG_MOV_AVG]])
            if signal:
                markets_with_signal.append(market)
                signals.append(signal)

        df_market_signal = pd.DataFrame({MARKET: markets_with_signal,
                                         TRADING_SIGNAL: signals},
                                        columns=[MARKET, TRADING_SIGNAL])
        return df_market_signal

    def calculate_signals_simulation(self, data, column):
        """
        function for backtesting, creates signals for every tick
        """
        data_with_signals = pd.DataFrame()
        markets = data[MARKET].unique()
        for market in markets:
            data_market = data[data[MARKET] == market].reset_index()
            data_market = data_market[0:LONG_WINDOW]
            data_market[SHORT_MOV_AVG] = data_market[column].rolling(window=SHORT_WINDOW, min_periods=1).mean()
            data_market[LONG_MOV_AVG] = data_market[column].rolling(window=LONG_WINDOW, min_periods=1).mean()

            data_market[TRADING_SIGNAL] = data_market[[SHORT_MOV_AVG, LONG_MOV_AVG]].apply(self.buy_sell_indicator,
                                                                                           axis=1)

            data_with_signals = pd.concat([data_with_signals, data_market])

        return data_with_signals

