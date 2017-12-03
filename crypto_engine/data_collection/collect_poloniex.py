from crypto_engine.exchanges.poloniex import Poloniex
from crypto_engine.data_collection.poloniex_websocket import PoloniexWebsocket
from crypto_engine.db.database import PoloniexTicker, POLONIEX, DB_FORMAT
from time import sleep


def ticker_data_collection():
    poloniex = Poloniex()
    time_to_sleep = poloniex.get_time_limit()

    while True:
        try:
            df = poloniex.get_tickers(retry=True)
            PoloniexTicker.save_table(table_name=DB_FORMAT.format(type='ticker', exchange=POLONIEX.lower()),
                                      df=df)
            sleep(time_to_sleep)

        except Exception as e:
            print('Exception {} occurred during poloniex ticker data collection'.format(e))


def ticker_websocket_data_collection():
    websocket_ticker = PoloniexWebsocket()
    websocket_ticker.start()
    while True:
        pass
    websocket_ticker.stop()
