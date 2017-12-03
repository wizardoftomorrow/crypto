from crypto_engine.exchanges.common import EXCHANGES_OBJ


class DataHandler(object):
    """
    DataHandler class deals with data collection from exchanges,
    signaling that data is collected and preparing data for database storage
    and on fly usage for 'real-time' strategies.

    TODO: include strong data analysis, to deal with possibly faulty data from exchanges
    TODO: make it possible to give exchanges and create only those objects
    """
    def __init__(self, exchanges=None):
        # set exchange objects for data handler
        if exchanges:
            self.exchanges = exchanges
        else:
            self.exchanges = EXCHANGES_OBJ
        self.collected_data = {}
        self.exchanges_collected = []

    def collect_ticker_data(self):
        """
        collects ticker data, returns list of exchanges for which data is collected

        """
        # reset collected data for this circle
        self.collected_data = {}
        for exchange in self.exchanges:
            try:
                # data is dataframe
                print(exchange)
                data = exchange.get_tickers()
                if not data.empty:
                    self.collected_data[exchange.get_name()] = data
            except:
                'Failed to collect data for {} name'.format(exchange.get_name())

        self.exchanges_collected = list(self.collected_data.keys())
        return self.exchanges_collected

    def get_exchanges_collected(self):
        # return list of exchanges for which data successfully collected in last data collection
        return self.exchanges_collected

    def get_data(self):
        return self.collected_data

    def get_exchange_data(self, exchange):
        return self.collected_data[exchange]




