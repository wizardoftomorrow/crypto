import pandas as pd
from crypto_engine.strategy.strategy import Strategy, BUY, SELL
from crypto_engine.common import MARKET, TRADING_SIGNAL, EXCHANGES, FEES, MAKER, TAKER
from itertools import permutations


EXCH_ASK = '{EXCHANGE}_ASK'
EXCH_BID = '{EXCHANGE}_BID'
EXCH_PAIRS = '{}_{}'
PERC_THRESHOLD = 1


class Arbitrage(Strategy):
    """
    Basic arbitrage strategy.
    At the moment supports only two exchanges,
    using pandas DataFrames could be easily extended to support multiple exchanges
    """

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
        return data >= PERC_THRESHOLD

    @staticmethod
    def calculate_profit_perc(data):
        """
        data[0] - ask(buy) price
        data[1] - bid(sell) price
        """
        perc_profit = (float(data[1]) - float(data[0])) / float(data[1]) * 100.00
        if perc_profit > 0:
            return perc_profit

    def calculate_signals(self, data, exchanges):
        """
        Argument:
            - DataFrame containing columns: MARKET, and EXCHANGE_ASK, EXCHANGE_BID for each exchange
            - exchanges

        Returns:
            Dataframe containing:
             MARKETS: for which arbitrage opportunity is spotted,
             signals in form  BUY and SELL columns - containing exchanges to buy/sell from
        """

        exchange_pairs = permutations(exchanges, 2)
        markets = []
        buy_from_exch = []
        sel_on_exch = []

        for pairs in exchange_pairs:
            pair_column = EXCH_PAIRS.format(pairs[0], pairs[1])
            data[pair_column] = data[[EXCH_ASK.format(EXCHANGE=pairs[0]),
                                      EXCH_BID.format(EXCHANGE=pairs[1])]].apply(self.calculate_profit_perc, axis=1)

            data_opportunity = data[data[pair_column].notnull()]
            fees = FEES[pairs[0][TAKER]] + FEES[pairs[1][MAKER]]
            data_opportunity[pair_column] = data_opportunity[pair_column].sub(fees)
            
            # leave only values which satisfy threshold for opportunity after fees
            data_opportunity = data_opportunity[data_opportunity[pair_column].map(self.buy_sell_indicator)]

            markets_with_opportunity = data_opportunity[MARKET].values.tolist()
            markets_with_opportunity_len = len(markets_with_opportunity)
            if markets_with_opportunity_len > 0:
                markets += markets_with_opportunity
                buy_from_exch += [pairs[0]] * markets_with_opportunity_len
                sel_on_exch += [pairs[1]] * markets_with_opportunity_len

            data.drop(pair_column, axis=1, inplace=True)

        arbitrage_opportunity_df = pd.DataFrame({MARKET: markets,
                                                 BUY: buy_from_exch,
                                                 SELL: sel_on_exch},
                                                columns=[MARKET, BUY, SELL])
        return arbitrage_opportunity_df

    def calculate_signals_simulation(self, data):
        """
        function for backtesting, creates signals for every tick
        """
        exchange_pairs = permutations(EXCHANGES, 2)

        for pairs in exchange_pairs:
            pair_column = EXCH_PAIRS.format(pairs[0], pairs[1])
            data[pair_column] = data[[EXCH_ASK.format(EXCHANGE=pairs[0]),
                                      EXCH_BID.format(EXCHANGE=pairs[1])]].apply(self.calculate_profit_perc, axis=1)

            fees = FEES[pairs[0][TAKER]] + FEES[pairs[1][MAKER]]
            data[pair_column] = data[pair_column].sub(fees)

            # creates additional signal column
            signal_column = EXCH_PAIRS.format(pair_column, TRADING_SIGNAL)
            data.loc[data[pair_column].map(self.buy_sell_indicator), signal_column] = 'BUY_SELL'

        return data

