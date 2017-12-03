from crypto_engine.strategy.strategy import Strategy, BUY, SELL
from crypto_engine.common import TRADING_SIGNAL


# TODO: implement targeted test strategy

class TStrategy(Strategy):
    """
    class TStrategy is used to test whether backtester/real_time trading system is
    behaving as expected.
    """
    def buy_indicator(self, data):
        """
        has no functionality
        """
        return BUY

    def sell_indicator(self, data):
        """
        has no functionality
        """
        return SELL

    def calculate_signals(self, data):
        """
        TODO: implement several different test signal generations
        """
        indexes_5 = list(range(0,  data.shape[0], 5))
        print(indexes_5)
        indexes_others = [i for i in data.index.tolist() if i not in indexes_5]
        data.loc[indexes_5, TRADING_SIGNAL] = BUY
        data.loc[indexes_others, TRADING_SIGNAL] = SELL

        return data




