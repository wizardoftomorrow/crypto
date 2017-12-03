from crypto_engine.exchanges.exchange import Exchange
from crypto_engine.common import (POLONIEX, MARKET, TIME, ASK, BID, BASE_VOLUME, QUOTE_VOLUME,
                                  HIGH, LOW, OPEN, CLOSE)
from datetime import datetime
import pandas as pd

POLONIEX_BASE = 'https://poloniex.com/public?command={command}&'

MAKER = 'maker'
TAKER = 'taker'

FEES = {MAKER: 0.15,
        TAKER: 0.25}

# limit is 6 calls per second, for precaution, we divide it by 5
TIME_LIMIT = 1 / 5

KEEP_COLUMNS = [MARKET, 'lowestAsk', 'highestBid', 'baseVolume', 'quoteVolume', TIME]
COLUMN_MAPPING = {'lowestAsk': ASK,
                  'highestBid': BID,
                  'baseVolume': BASE_VOLUME,
                  'quoteVolume': QUOTE_VOLUME}

KEEP_COLUMNS_CANDLE = ['high', 'low', 'open', 'close', 'volume', 'quoteVolume']
COLUMN_MAPPING_CANDLE = {'high': HIGH,
                         'low': LOW,
                         'open': OPEN,
                         'close': CLOSE,
                         'volume': BASE_VOLUME,
                         'quoteVolume': QUOTE_VOLUME}


class Poloniex(Exchange):
    """
    Used for requesting Poloniex
    providing api_key, api_secret, additional (except public) api calls can be performed
    """

    def __init__(self,
                 base_url=POLONIEX_BASE,
                 exchange_name=POLONIEX,
                 time_limit=TIME_LIMIT):
        super(Poloniex, self).__init__(base_url, exchange_name, time_limit)

    def create_request(self, command):
        return self.base_url.format(command=command)

    @staticmethod
    def get_markets():
        """
        not supported, if needed can be accomplished using e.g. get_ticker
        """
        pass

    def get_tickers(self, retry=False):
        """
        returns ticker data for all markets
        """
        if retry:
            resp = self.query_with_retry(command='returnTicker')
        else:
            resp = self.query(command='returnTicker')

        ticker_data = pd.pandas.DataFrame.from_dict(resp, orient='index')
        ticker_data[MARKET] = ticker_data.index
        ticker_data.reset_index(drop=True, inplace=True)
        ticker_data[TIME] = [(datetime.now().replace(microsecond=0))] * ticker_data.shape[0]
        ticker_data = ticker_data[KEEP_COLUMNS]
        ticker_data.rename(columns=COLUMN_MAPPING, inplace=True)
        return ticker_data

    def get_24volume(self):
        resp = self.query(command='return24hVolume')
        # take only dict
        dict_data = {element: resp[element] for element in resp if isinstance(resp[element], dict)}

        # rename keys
        for element in dict_data:
            dict_keys = list(dict_data[element])
            dict_data[element][BASE_VOLUME] = dict_data[element].pop(dict_keys[0])
            dict_data[element][QUOTE_VOLUME] = dict_data[element].pop(dict_keys[1])

        volume_data = pd.pandas.DataFrame.from_dict(dict_data, orient='index')
        volume_data[MARKET] = volume_data.index
        return volume_data

    def get_candlestick(self, market, tick_interval, start=None, end=None, retry=False):
        """
        returns candle data for specific market
        """
        if retry:
            resp = self.query_with_retry(command='returnChartData',
                                         args={'currencyPair': market,
                                               'period': tick_interval,
                                               'start': start,
                                               'end': end})
        else:
            resp = self.query(command='returnChartData',
                              args={'currencyPair': market,
                                    'period': tick_interval,
                                    'start': start,
                                    'end': end})

        ticker_data = pd.pandas.DataFrame.from_dict(resp)
        ticker_data[MARKET] = [market] * ticker_data.shape[0]
        ticker_data[TIME] = [(datetime.now().replace(microsecond=0))] * ticker_data.shape[0]
        ticker_data = ticker_data[KEEP_COLUMNS_CANDLE]
        ticker_data.rename(columns=COLUMN_MAPPING_CANDLE, inplace=True)
        return ticker_data

    def get_order_book(self, market, depth=10):
        resp = self.query(command='returnOrderBook',
                          args={'currencyPair': market,
                                'depth': depth})
        return resp

    def get_maker_fee(self):
        return FEES[MAKER]

    def get_taker_fee(self):
        return FEES[TAKER]
