from crypto_engine.db.common import BITTREX, POLONIEX
from crypto_engine.db.database import PoloniexTicker, BittrexTicker, db_engine, DB_FORMAT
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from time import time
import pandas as pd

Session = sessionmaker(bind=db_engine)
session = Session()


EXCHANGE_DB = {POLONIEX: PoloniexTicker,
               BITTREX: BittrexTicker}


# TODO: wrap pull functions; logging


class DatabaseHandler(object):
    """
    DatabaseHandler class deals with storing and retrieving data from database

    """
    def __init__(self):
        self.exchanges_db = {POLONIEX: PoloniexTicker,
                             BITTREX: BittrexTicker}
        self.exchanges = [POLONIEX, BITTREX]

    def save_ticker_data(self, data):
        """
        saves ticker data to database,
        expects data in form of dictionaries with exchange names and DataFrame
        with ticker data

        """

        for exchange, ticker_data in data.iteritems():
            if exchange in self.exchanges:
                try:
                    self.exchanges_db[exchange].save_table(table_name=DB_FORMAT.format(type='ticker',
                                                                                       exchange=exchange.lower()),
                                                           data=ticker_data)
                except:
                    print('Time: {} \n'
                          'Fail to save ticker data for exchange {}'.format(time(), exchange))

            else:
                print('Unknown exchange:, ', exchange)

    def pull_market_data(self, exchange, market, data_size):
        if exchange in self.exchanges:
            try:
                exc_db_obj = EXCHANGE_DB[exchange]
                query = session.query(exc_db_obj).filter_by(market=market).order_by(exc_db_obj.id).limit(data_size)

                return pd.read_sql(query.statement, db_engine)
            except:
                print('Fail to fetch data for {} exchange, {} market'.format(exchange, market))

        else:
            print('Unknown exchange: ', exchange)

    def pull_markets_data(self, exchange, markets):
        """
        market is list
        """
        if exchange in self.exchanges:
            try:
                query = session.query(EXCHANGE_DB[exchange]).filter(EXCHANGE_DB[exchange].market.in_(markets))
                return pd.read_sql(query.statement, db_engine)
            except:
                print('Fail to save pull data for exchange {}'.format(time(), exchange))

        else:
            print('Unknown exchange: ', exchange)

    def get_all_in_timeframe(self, exchange, time_start, time_end):
        """
        time_start and time_end are datetime objects
        TODO: error handling
        """
        if exchange in self.exchanges:
            try:
                exch_object_db = EXCHANGE_DB[exchange]
                query = session.query(exch_object_db).filter(and_(exch_object_db.time >= time_start,
                                                                  exch_object_db.time <= time_end))
                return pd.read_sql(query.statement, db_engine)
            except:
                print('Fail to save pull data for exchange {}'.format(time(), exchange))
        else:
            print('Unknown exchange: ', exchange)

    def get_market_in_timeframe(self, exchange, market, time_start, time_end):
        """
        time_start and time_end are datetime objects
        TODO: error handling
        """
        if exchange in self.exchanges:
            try:
                exch_object_db = EXCHANGE_DB[exchange]
                query = session.query(exch_object_db).filter(and_(exch_object_db.time >= time_start,
                                                                  exch_object_db.time <= time_end,
                                                                  exch_object_db.market == market))
                return pd.read_sql(query.statement, db_engine)

            except:
                print('Fail to save pull data for exchange {}'.format(time(), exchange))
        else:
            print('Unknown exchange: ', exchange)

    def get_record_by_id(self, exchange, rec_id):
        if exchange in self.exchanges:
            try:
                query = session.query(EXCHANGE_DB[exchange]).filter_by(id=rec_id)
                return pd.read_sql(query.statement, db_engine)

            except:
                print('Fail to fetch data for exchange {}, id {}'.format(exchange, rec_id))

        else:
            print('Unknown exchange: ', exchange)

    def get_distinct_markets(self, exchange):
        if exchange in self.exchanges:
            try:
                query = session.query(EXCHANGE_DB[exchange].market.distinct().label('market'))
                return [market.market for market in query.all()]
            except:
                print('Fail to save pull data for exchange {}'.format(time(), exchange))
        else:
            print('Unknown exchange: ', exchange)
