import time
import hmac
import requests
from urllib.parse import urlencode
import hashlib

BITTREX_BASE = 'https://bittrex.com/api/v2.0/{method_set}/{method}?'
EMPTY_VALUE = ''
PUBLIC_QUERIES = 'pub'

TICK_INTERVALS = {'One_Minute': 'OneMin',
                  'Five_Minutes': 'fiveMin',
                  'Thirty_Minutes': 'thirtyMin',
                  'Hour': 'hour',
                  'Day': 'Day'}


def using_requests(request_url, apisign):
    return requests.get(
        request_url,
        headers={"apisign": apisign},
        timeout=60).json()


class Bittrex(object):
    """
    Used for requesting Bittrex
    providing api_key, api_secret, additional (except public) api calls can be performed
    """

    def __init__(self, api_key=None, api_secret=None, dispatch=using_requests):
        self.api_key = str(api_key) if api_key else EMPTY_VALUE
        self.api_secret = str(api_secret) if api_secret else EMPTY_VALUE
        self.dispatch = dispatch

    def api_query(self, method, options=None):
        """
        query frame for calling different api methods
        """

        if not options:
            options = {}
        nonce = str(int(time.time() * 1000))

        method_set = PUBLIC_QUERIES

        request_url = BITTREX_BASE.format(method_set=method_set, method=method)

        if method_set != PUBLIC_QUERIES:
            # provide key, at the moment only public queries are used
            request_url = "{0}apikey={1}&nonce={2}&".format(
                request_url, self.api_key, nonce)

        request_url += urlencode(options)

        apisign = hmac.new(self.api_secret.encode(),
                           request_url.encode(),
                           hashlib.sha512).hexdigest()

        return self.dispatch(request_url, apisign)

    def get_markets(self):
        """
        Used to get the open and available trading markets
        at Bittrex along with other meta data.
        Endpoint: /pub/Markets/GetMarkets

        Example of query result:
            {'success': True,
             'message': '',
             'result': [ {'MarketCurrency': 'LTC',
                          'BaseCurrency': 'BTC',
                          'MarketCurrencyLong': 'Litecoin',
                          'BaseCurrencyLong': 'Bitcoin',
                          'MinTradeSize': 1e-08,
                          'MarketName': 'BTC-LTC',
                          'IsActive': True,
                          'Created': '2014-02-13T00:00:00',
                          'Notice': None,
                          'IsSponsored': None,
                          'LogoUrl': 'https://i.imgur.com/R29q3dD.png'},
                          ...
                        ]
            }

        """
        return [market['MarketName'] for market in self.api_query(method='Markets/GetMarkets')['result']
                if market['IsActive']]

    def get_market_summaries(self):
        """
        return the last 24 hour summary of all active exchanges

        Endpoint: /pub/Markets/GetMarketSummaries
        """
        return self.api_query(method='Markets/GetMarketSummaries')

    def get_market_summary(self, market):
        """
        Used to get the last 24 hour summary of all active
        exchanges in specific market(coin)

        Endpoint: Market/GetMarketSummar
        """
        return self.api_query(method='Market/GetMarketSummary',
                              options={'market': market,
                                       'marketname': market})

    def list_markets_by_currency(self, currency):
        """
        Helper function to see which markets exist for a currency.
        """
        return [market for market in self.get_markets()
                if market.lower().startswith(currency.lower())]

    def get_candles(self, market, tick_interval):
        """
        Used to get all tick candle for a market.

        Endpoint: pub/market/GetTicks

        Example of query result:
            { success: true,
              message: '',
              result:
               [ { O: 421.20630125,
                   H: 424.03951276,
                   L: 421.20630125,
                   C: 421.20630125,
                   V: 0.05187504,
                   T: '2016-04-08T00:00:00',
                   BV: 21.87921187 },
                 { O: 420.206,
                   H: 420.206,
                   L: 416.78743422,
                   C: 416.78743422,
                   V: 2.42281573,
                   T: '2016-04-09T00:00:00',
                   BV: 1012.63286332 }]
            }
        """

        return self.api_query(method='/market/GetTicks',
                              options={'marketName': market,
                                       'tickInterval': tick_interval})

