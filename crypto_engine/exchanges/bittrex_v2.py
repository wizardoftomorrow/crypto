from crypto_engine.exchanges.exchange import Exchange
from crypto_engine.common import BITTREX, MARKET, CLOSE, HIGH, LOW, VOLUME, TIME
import logging
import pandas as pd

BITTREX_BASE = 'https://bittrex.com/api/v2.0/pub/{command}?'
RESULT = 'result'
SUCCESS = 'success'

TICK_INTERVALS = {'One_Minute': 'OneMin',
                  'Five_Minutes': 'fiveMin',
                  'Thirty_Minutes': 'thirtyMin',
                  'Hour': 'hour',
                  'Day': 'Day'}

LOG = logging.getLogger('application')
logging.basicConfig(level=logging.DEBUG)

MAKER = 'maker'
TAKER = 'taker'

FEES = {MAKER: 0.25,
        TAKER: 0.25}

TIME_LIMIT = 1

KEEP_COLUMNS = [MARKET, 'C', 'H', 'L', 'V', 'T']

COLUMN_MAPPING = {'C': CLOSE,
                  'H': HIGH,
                  'L': LOW,
                  'V': VOLUME,
                  'T': TIME}


def rename_market_data(market):
    """
    allows input to be in general form PAIR_PAIR
    """
    markets = str(market).split('_')
    return '-'.join(markets)


def rename_from_market_data(market):
    """
    wraps market data to form PAIR_PAIR
    """
    markets = str(market).split('-')
    return '_'.join(markets)


class Bittrex_2(Exchange):
    """
    Used for requesting Bittrex, based on v2 version, which is not yet officially available.
    It uses only public part of the api.

    providing api_key, api_secret, additional (except public) api calls can be performed,
    but for usages at the moment it is not necessary.
    """

    def __init__(self,
                 base_url=BITTREX_BASE,
                 exchange_name=BITTREX,
                 time_limit=TIME_LIMIT):
        super(Bittrex_2, self).__init__(base_url, exchange_name, time_limit)

    def create_request(self, command):
        return self.base_url.format(command=command)

    def _get_markets(self):
        """
        Used to get the open and available trading markets
        at Bittrex along with other meta data.
        1.1 Endpoint: /public/getmarkets
        """
        resp = self.query(command='Markets/GetMarkets')
        if resp[SUCCESS]:
            return [market['MarketName'] for market in resp[RESULT]
                    if market['IsActive']]

    def get_markets(self):
        markets = self._get_markets()
        return list(map(rename_from_market_data, markets))

    @staticmethod
    def get_ticker():
        """
        not supported in v2
        """
        return

    def _get_candlestick(self, market, tick_interval):
        """
        function specific for V2 api

        """

        resp = self.query(command='/market/GetTicks',
                          args={'marketName': market,
                                'tickInterval': tick_interval})
        if resp[SUCCESS]:
            return resp[RESULT]

    def get_candlestick(self, market, tick_interval, start=None, end=None, retry=False):
        """
            wrapper to transform market format

        """
        market = rename_market_data(market)
        return self._get_candlestick(market, tick_interval)

    def get_candles(self, tick_interval, start=None, end=None):
        """
        function specific for V2 api

        """
        markets = self._get_markets()
        df = pd.DataFrame(columns=KEEP_COLUMNS)
        for market in markets:
            data_candle = pd.DataFrame(self._get_candlestick(market=market,
                                                             tick_interval=tick_interval))
            data_candle[MARKET] = rename_from_market_data(market)
            data_candle = data_candle[KEEP_COLUMNS].rename(COLUMN_MAPPING)
            print(data_candle)
            df = pd.concat([df, data_candle], ignore_index=True)

        return df

    def get_24volume(self):
        resp = self.query(command='Markets/GetMarketSummaries')
        if resp[SUCCESS]:
            return [{'pair': summary['Summary']['MarketName'], 'volume': summary['Summary']['Volume']}
                    for summary in resp[RESULT]]

    def get_order_book(self, market):
        resp = self.query(command='Market/GetMarketOrderBook',
                          args={'market': market, 'type': 'both'})

        if resp[SUCCESS]:
            return resp[RESULT]

    def get_tickers(self, retry=False):
        """
        bittrex v2 does not provide tickers
        """
        return

    def get_maker_fee(self):
        return FEES[MAKER]

    def get_taker_fee(self):
        return FEES[TAKER]
