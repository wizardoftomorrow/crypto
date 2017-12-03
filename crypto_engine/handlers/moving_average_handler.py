from crypto_engine.handlers.strategy_handler import StrategyHandler
from crypto_engine.strategy.common import MOVING_AVERAGE
from crypto_engine.strategy.moving_average import LONG_WINDOW
from crypto_engine.common import ASK, BID, TRADING_SIGNAL, MARKET
from collections import OrderedDict
import pandas as pd


EXCH_ASK = '{EXCHANGE}_ASK'
EXCH_BID = '{EXCHANGE}_BID'


EXCHANGE = 'EXCHANGE'
BUY = 'BUY'
SELL = 'SELL'


class MovingAverageHandler(StrategyHandler):
    """
    Arbitrage strategy handler
    """

    def __init__(self, data_handler, strategy_name=MOVING_AVERAGE):
        super(MovingAverageHandler, self).__init__(strategy_name, data_handler)

    def _get_data(self, exchange, markets):
        if not markets:
            markets = self.data_handler.get_distinct_markets()

        # TODO: optimize data_size query
        return self.data_handler.pull_markets_data(exchange=exchange,
                                                   markets=markets,
                                                   data_size=len(markets)*LONG_WINDOW)

    def prepare_output(self, strategy_results):
        exchange_output = []
        unique_exchanges = strategy_results[EXCHANGE].unique().tolist()
        for exchange in unique_exchanges:
            data_exchange = strategy_results[strategy_results[EXCHANGE] == exchange]
            markets_buy = data_exchange[data_exchange[TRADING_SIGNAL] == BUY][MARKET].values.tolist()
            markets_sell = data_exchange[data_exchange[TRADING_SIGNAL] == SELL][MARKET].values.tolist()
            signals = OrderedDict(BUY=markets_buy,
                                  SELL=markets_sell)

            exchange_signals = OrderedDict(EXCHANGE=exchange,
                                           SIGNALS=signals)
            exchange_output.append(exchange_signals)

        strategy_res = OrderedDict(STRATEGY=self.get_name(),
                                   EXCHANGES=exchange_output)

        return strategy_res

    def execute_strategy(self, exchanges, markets=None):
        """
        main function for strategy execution

        knows how to get data appropriately and which strategy to execute
        """
        strategy_results = pd.DataFrame(columns=[EXCHANGE, MARKET, TRADING_SIGNAL])
        for exchange in exchanges:
            data, markets = self._get_data(exchange, markets)
            strategy_res = self.strategy.calculate_signals(data, markets, column=ASK)
            strategy_res[EXCHANGE] = [exchange] * strategy_res.shape[0]

            if not strategy_res.empty:
                strategy_results = pd.concat([strategy_results, strategy_res], ignore_index=True)

        if strategy_results.empty:
            return None

        return self.prepare_output(strategy_results)


