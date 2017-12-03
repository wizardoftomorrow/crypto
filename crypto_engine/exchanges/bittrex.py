from crypto_engine.exchanges.exchange import Exchange
from crypto_engine.common import BITTREX, MARKET, TIME, ASK, BID, BASE_VOLUME, QUOTE_VOLUME
import pandas as pd
from datetime import datetime

BITTREX_BASE = 'https://bittrex.com/api/v1.1/public/{command}?'
RESULT = 'result'
SUCCESS = 'success'


MAKER = 'maker'
TAKER = 'taker'
FEES = {MAKER: 0.25,
        TAKER: 0.25}

TIME_LIMIT = 1

KEEP_COLUMNS = [MARKET, 'Ask', 'Bid', TIME]

COLUMN_MAPPING = {'Ask': ASK,
                  'Bid': BID}


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


class Bittrex(Exchange):
    """
    Used for requesting Bittrex, based on current stable v1.1 version.
    It uses only public part of the api.

    providing api_key, api_secret, additional (except public) api calls can be performed,
    but for usages at the moment it is not necessary.
    """

    def __init__(self,
                 base_url=BITTREX_BASE,
                 exchange_name=BITTREX,
                 time_limit=TIME_LIMIT):
        super(Bittrex, self).__init__(base_url, exchange_name, time_limit)

    def create_request(self, command):
        return self.base_url.format(command=command)

    def _get_markets(self):
        """
        Used to get the open and available trading markets
        at Bittrex along with other meta data.
        1.1 Endpoint: /public/getmarkets
        """
        resp = self.query(command='getmarkets')
        if resp[SUCCESS]:
            return [market['MarketName'] for market in resp[RESULT]
                    if market['IsActive']]

    def get_markets(self):
        """
        wrapper to return expected format of markets
        """
        markets = self._get_markets()
        return list(map(rename_from_market_data, markets))

    def _get_ticker(self, market):
        market = rename_market_data(market)
        resp = self.query(command='getticker',
                          args={'market': market})

        if resp[SUCCESS]:
            return resp[RESULT]

    def get_ticker(self, market):
        """
        wrapper to accept general market format
        """
        market = rename_market_data(market)
        return self._get_ticker(market)

    def get_24volume(self):
        resp = self.query(command='getmarketsummaries')
        if resp[SUCCESS]:
            return [{'pair': summary['MarketName'], 'volume': summary['Volume']}
                    for summary in resp[RESULT]]

    def get_order_book(self, market):
        market = rename_market_data(market)
        resp = self.query(command='getorderbook',
                          args={'market': market, 'type': 'both'})
        if resp[SUCCESS]:
            return resp[RESULT]

    def get_tickers(self, retry=False):
        """
        bittrex does not provide api method for fetching all tickers in one call
        Note: calling get_ticker for each market results in nr_markets * limit_time duration
        """
        markets = self._get_markets()
        df = pd.DataFrame(columns=[MARKET, BID, ASK, BASE_VOLUME, QUOTE_VOLUME, TIME])
        for market in markets[:3]:
            ticker_data = pd.DataFrame(self._get_ticker(market), index=[0])
            ticker_data[MARKET] = rename_from_market_data(market)
            ticker_data[TIME] = datetime.now().replace(microsecond=0)
            ticker_data = ticker_data[KEEP_COLUMNS]
            ticker_data.rename(columns=COLUMN_MAPPING, inplace=True)
            ticker_data[BASE_VOLUME] = ''
            ticker_data[QUOTE_VOLUME] = ''
            df = pd.concat([df, ticker_data], ignore_index=True)

        return df

    def get_candlestick(self, market, tick_interval, start=None, end=None, retry=False):
        """
        not supported in v1.1
        """
        return

    def get_maker_fee(self):
        return FEES[MAKER]

    def get_taker_fee(self):
        return FEES[TAKER]
