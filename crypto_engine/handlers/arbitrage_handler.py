from collections import OrderedDict

import pandas as pd

from crypto_engine.common import MARKET, ASK, BID
from crypto_engine.handlers.strategy_handler import StrategyHandler
from crypto_engine.strategy.common import ARBITRAGE

EXCH_ASK = '{EXCHANGE}_ASK'
EXCH_BID = '{EXCHANGE}_BID'

BUY = 'BUY'
SELL = 'SELL'


class ArbitrageHandler(StrategyHandler):
    """
    Arbitrage strategy handler

    TODO: optimize

    """

    def __init__(self, data_handler, strategy_name=ARBITRAGE):
        super(ArbitrageHandler, self).__init__(strategy_name, data_handler)

    def _get_data(self):
        """
        main function for data retrieving using data_handler
        """

        exchanges = self.data_handler.get_exchanges_collected()
        markets = []
        for exchange in exchanges:
            data = self.data_handler.get_exchange_data(exchange)
            markets += data[MARKET].unique().tolist()

        # take only markets with data in collected data
        # TODO: maybe reevaluate this to use previous value(s) as well???
        duplicate_markets = [market for market in set(markets) if markets.count(market) > 1]

        final_df = pd.DataFrame()
        exchanges_relevant = []
        for exchange in exchanges:
            data = self.data_handler.get_exchange_data(exchange)
            data = data[data[MARKET].isin(duplicate_markets)]
            if not data.empty:
                data = data[[MARKET, ASK, BID]].rename(
                    columns={ASK: EXCH_ASK.format(EXCHANGE=exchange),
                             BID: EXCH_BID.format(EXCHANGE=exchange)})

                if final_df.empty:
                    final_df = data
                else:
                    final_df = final_df.merge(data, on=MARKET)

                exchanges_relevant.append(exchange)

        return final_df, exchanges_relevant

    def prepare_output(self, strategy_results):
        exchange_output = []
        unique_exchanges = strategy_results[BUY].unique().tolist() + strategy_results[SELL].unique().tolist()
        for exchange in unique_exchanges:
            markets_buy = strategy_results[strategy_results[BUY] == exchange][MARKET].values.tolist()
            markets_sell = strategy_results[strategy_results[SELL] == exchange][MARKET].values.tolist()
            signals = OrderedDict(BUY=markets_buy,
                                  SELL=markets_sell)

            exchange_signals = OrderedDict(EXCHANGE=exchange,
                                           SIGNALS=signals)
            exchange_output.append(exchange_signals)

        strategy_res = OrderedDict(STRATEGY=self.get_name(),
                                   EXCHANGES=exchange_output)

        return strategy_res

    def execute_strategy(self):
        """
        main function for strategy execution

        knows how to get data and which strategy to execute
        """
        data, exchanges = self._get_data()
        strategy_res = self.strategy.calculate_signals(data, exchanges)
        if strategy_res.empty:
            return None

        return self.prepare_output(strategy_res)
