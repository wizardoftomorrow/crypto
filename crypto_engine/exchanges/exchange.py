import time
import abc
from functools import wraps
import requests
from urllib.parse import urlencode
import logging

# value set based on reddit comments
TIMEOUT = 75
# retry for 5 minutes
RETRY_NR = 300
ERROR = 'error'
LOG = logging.getLogger('exchange')
logging.basicConfig(level=logging.DEBUG)


# if once needed for header creation
def nonce():
    # creates nonce for api authentication
    return str(int(time.time() * 1000))


def get_data(request_url, timeout):
    response = requests.get(
        request_url,
        timeout=timeout)
    response.raise_for_status()

    if ERROR in response.json():
        raise requests.RequestException('Error returned in response')
    # Everything went OK
    return response.json()


def _retry(func):
    """
    retry decorator
    """

    @wraps(func)
    def retrying(*args, **kwargs):
        for delay in range(RETRY_NR):
            try:
                # attempt call
                return func(*args, **kwargs)

            except Exception as e:
                # log exception and wait
                LOG.debug(e)
                LOG.info('{}. attempt to connect. Delaying for {} s.'.format(delay + 1, 1))
                time.sleep(1)

    return retrying


class Exchange(object):
    """
    Exchange class is base class for all exchange objects.
    If once needed, additional abstract methods should be added.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, base_url,
                 exchange_name,
                 time_limit,
                 timeout=TIMEOUT):
        self.base_url = base_url
        self.exchange_name = exchange_name
        self.timeout = timeout
        self.time_limit = time_limit
        self.timestamp = None

    def get_name(self):
        return self.exchange_name

    @_retry
    def query_with_retry(self, command, args=None):
        request_url = self.create_request(command)
        if args:
            request_url += urlencode(args)
        return get_data(request_url, self.timeout)

    def query(self, command, args=None):
        """
        to be used without retry, and with time delay not to reach limitations
        """

        request_url = self.create_request(command)
        if args:
            request_url += urlencode(args)

        if self.timestamp:
            time_passed = time.time() - self.timestamp
            if time_passed < self.time_limit:
                time.sleep(self.time_limit - time_passed)

        # save new time of api call
        self.timestamp = time.time()
        try:
            return get_data(request_url, self.timeout)
        except Exception as e:
            # log exception and wait
            LOG.debug(e)
            return None

    def get_time_limit(self):
        return self.time_limit

    @abc.abstractmethod
    def create_request(self, command):
        return

    @abc.abstractmethod
    def get_tickers(self, retry=False):
        return

    @abc.abstractmethod
    def get_24volume(self):
        return

    @abc.abstractmethod
    def get_order_book(self, market):
        return

    @abc.abstractmethod
    def get_maker_fee(self):
        return

    @abc.abstractmethod
    def get_taker_fee(self):
        return

    @abc.abstractmethod
    def get_candlestick(self, market, tick_interval, start=None, end=None, retry=False):
        return
