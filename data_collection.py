#!/usr/bin/python
import pandas as pd
from bittrex import Bittrex, TICK_INTERVALS
import database

columns_mapping = {'C': 'close',
                   'H': 'high',
                   'L': 'low',
                   'V': 'volume',
                   'T': 'time'}


bittrex_obj = Bittrex()


def collect_markets():
    markets = bittrex_obj.get_markets()
    markets_df = pd.DataFrame({'id': range(len(markets)),
                               'market_name': markets},
                              columns=['id', 'market_name'])

    database.Markets.save_table(markets_df)


def collect_candle_data():
    market_data = database.Markets.get_all()
    for _, row in market_data.iterrows():

        get_candles = bittrex_obj.get_candles(market=row['market_name'],
                                              tick_interval=TICK_INTERVALS['One_Minute'])
        if get_candles['success']:
            data = pd.DataFrame(get_candles['result'])[['C', 'H', 'L', 'V', 'T']].rename(columns=columns_mapping)
            data['market_id'] = [row['id']] * data.shape[0]
            database.Tickers.save_table(data)


if __name__ == '__main__':
    try:
        collect_markets()
        collect_candle_data()
        print('Data collected successfully')

    except Exception as e:
        print('Error {} during data collection'.format(e))

