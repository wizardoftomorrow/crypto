#!/usr/bin/python
import psycopg2
from sqlalchemy import create_engine, inspect, Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from crypto_engine.common import (BITTREX, POLONIEX, MARKET, ASK, BID,
                                  BASE_VOLUME, QUOTE_VOLUME,
                                  TIME, HIGH, LOW, OPEN, CLOSE)
from crypto_engine.db.config_db import config_db
import pandas as pd

DB_FORMAT = '{type}_{exchange}_db'

cnf = config_db()


def create_db_uri():
    return "postgres://{0}:{1}@{2}:{3}/{4}".format(cnf['user'],
                                                   cnf['password'],
                                                   cnf['host'],
                                                   cnf['port'],
                                                   cnf['database'])


# wait for 2 seconds before giving up on connection
db_engine = create_engine(create_db_uri(),
                          pool_size=20,
                          pool_timeout=2)

DeclarativeBase = declarative_base()


class Ticker(object):
    """
    mapper for ticker data
    # TODO: add additional queries, when necessary
    """
    id = Column(Integer, primary_key=True)
    market = Column(MARKET, String(10), nullable=False)
    ask = Column(ASK, Float(precison=16, scale=8), nullable=False)
    bid = Column(BID, Float(precison=16, scale=8), nullable=False)
    base_volume = Column(BASE_VOLUME, Float(precison=24, scale=8), nullable=False)
    quote_volume = Column(QUOTE_VOLUME, Float(precison=24, scale=8), nullable=False)
    time = Column(TIME, DateTime(timezone=True), nullable=False)

    def __init__(self, input_id, market, ask, bid, base_volume, quote_volume, time):
        self.id = input_id
        self.market = market
        self.ask = ask
        self.bid = bid
        self.base_volume = base_volume
        self.quote_volume = quote_volume
        self.time = time

    @staticmethod
    def save_table(table_name, df):
        df.to_sql(table_name,
                  con=db_engine,
                  if_exists='append',
                  index=False)
        print('Table successfully updated')

    @staticmethod
    def get_all(table_name):
        return pd.read_sql_table(table_name,
                                 db_engine)


class BittrexTicker(Ticker, DeclarativeBase):
    __tablename__ = DB_FORMAT.format(type='ticker', exchange=BITTREX.lower())


class PoloniexTicker(Ticker, DeclarativeBase):
    __tablename__ = DB_FORMAT.format(type='ticker', exchange=POLONIEX.lower())


class Candle(object):
    """
    mapper for candle data
    # TODO: add additional queries, when necessary
    """
    id = Column(Integer, primary_key=True)
    market = Column(MARKET, String(10), nullable=False)
    open = Column(OPEN, Float(precison=16, scale=8), nullable=False)
    close = Column(CLOSE, Float(precison=16, scale=8), nullable=False)
    high = Column(HIGH, Float(precison=16, scale=8), nullable=False)
    low = Column(LOW, Float(precison=16, scale=8), nullable=False)
    base_volume = Column(BASE_VOLUME, Float(precison=24, scale=8), nullable=False)
    quote_volume = Column(QUOTE_VOLUME, Float(precison=24, scale=8), nullable=False)
    time = Column(TIME, DateTime(timezone=True), nullable=False)

    def __init__(self, input_id, market, open_, close, high, low, base_volume, quote_volume, time):
        self.id = input_id
        self.market = market
        self.open = open_,
        self.close = close,
        self.high = high,
        self.low = low,
        self.base_volume = base_volume
        self.quote_volume = quote_volume
        self.time = time

    @staticmethod
    def save_table(table_name, df):
        df.to_sql(table_name,
                  con=db_engine,
                  if_exists='append',
                  index=False)
        print('Table successfully updated')

    @staticmethod
    def get_all(table_name):
        return pd.read_sql_table(table_name,
                                 db_engine)


class BittrexCandle(Candle, DeclarativeBase):
    __tablename__ = DB_FORMAT.format(type='candle', exchange=BITTREX.lower())


class PoloniexCandle(Candle, DeclarativeBase):
    __tablename__ = DB_FORMAT.format(type='candle', exchange=POLONIEX.lower())


def test_connection():
    """
    Connect to the PostgreSQL database server
    """
    conn = None
    try:
        # read connection parameters
        params = config_db()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        cur.close()

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def create_tables():
    try:
        DeclarativeBase.metadata.create_all(bind=db_engine)
        print('Tables for {} successfully created.'.format(cnf['database']))
    except (Exception, psycopg2.DatabaseError) as e:
        print('{} occurred during tables creation for {}'.format(e, cnf['database']))


def delete_tables():
    try:
        DeclarativeBase.metadata.drop_all(bind=db_engine)
        print('Tables for {} successfully deleted.'.format(cnf['database']))
    except (Exception, psycopg2.DatabaseError) as e:
        print('{} occurred during tables deletion for {}'.format(e, cnf['database']))


def check_everything():
    inspector = inspect(db_engine)
    tables = inspector.get_table_names()
    print('Healthy tables:', tables)
    for table in tables:
        print('Columns present in {} table: {}'.format(table, inspector.get_columns(table)))


if __name__ == '__main__':
    test_connection()
    # delete_tables()
    create_tables()
    check_everything()
