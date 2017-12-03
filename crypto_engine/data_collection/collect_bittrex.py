from crypto_engine.exchanges.bittrex import Bittrex
from crypto_engine.exchanges.bittrex_v2 import Bittrex_2, TICK_INTERVALS
from crypto_engine.db.database import DB_FORMAT, BITTREX, BittrexTicker, BittrexCandle
from time import sleep
import schedule


def collect_ticker_data():
    bittrex = Bittrex()
    time_to_sleep = bittrex.get_time_limit()
    while True:
        try:
            df = bittrex.get_tickers(retry=True)
            BittrexTicker.save_table(table_name=DB_FORMAT.format(type='ticker', exchange=BITTREX.lower()),
                                     df=df)

            sleep(time_to_sleep)

        except Exception as e:
            print('Exception {} occurred during bittrex ticker data collection'.format(e))


def collect_candle_data():
    bittrex = Bittrex_2()
    while True:
        try:
            df = bittrex.get_candles(tick_interval=TICK_INTERVALS['One_Minute'])
            BittrexCandle.save_table(table_name=DB_FORMAT.format(type='candle', exchange=BITTREX.lower()),
                                     df=df)

        except Exception as e:
            print('Exception {} occurred during bittrex ticker data collection'.format(e))


schedule.every(10).day.at('8:00').do(collect_candle_data())

