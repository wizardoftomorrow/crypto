#!/usr/bin/python
import psycopg2
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config_db import config
import pandas as pd


cnf = config()


def db_uri():
    return "postgres://{0}:{1}@{2}/{3}".format(cnf['user'],
                                               cnf['password'],
                                               cnf['host'],
                                               cnf['database'])


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

print(db_uri())
db = SQLAlchemy(app)


class Markets(db.Model):
    __tablename__ = 'markets'
    id = db.Column(db.INTEGER, primary_key=True)
    market_name = db.Column(db.VARCHAR(10), nullable=False)

    def __init__(self, id, market_name):
        self.id = id
        self.market_name = market_name

    def save(self):
        db.session.add(self)
        db.session.commit()

    @staticmethod
    def save_table(df):
        df.to_sql('markets',
                  con=db.engine,
                  if_exists='replace',
                  index=False)

    @staticmethod
    def get_by_id(id):
        return Markets.query.get(id).market_name

    @staticmethod
    def get_by_market_name(market_name):
        return Markets.query.filter_by(market_name=market_name).first().id

    @staticmethod
    def get_market_name_contains(contains):
        return pd.read_sql(Markets.query.filter(Markets.market_name.contains(contains)).statement,
                           con=db.engine)

    @staticmethod
    def get_market_id_with_names(market_names):
        return pd.read_sql(Markets.query.filter(Markets.market_name.in_(market_names)).statement,
                           con=db.engine)

    @staticmethod
    def get_all():
        return pd.read_sql_table('markets',
                                 db.engine,
                                 columns=['id', 'market_name'])


class Tickers(db.Model):
    __tablename__ = 'tickers'
    id = db.Column(db.INTEGER, primary_key=True)
    market_id = db.Column(db.INTEGER, nullable=False)
    high = db.Column(db.NUMERIC(16, 8), nullable=False)
    low = db.Column(db.NUMERIC(16, 8), nullable=False)
    close = db.Column(db.NUMERIC(16, 8), nullable=False)
    volume = db.Column(db.NUMERIC(24, 8), nullable=False)
    time = db.Column(db.DateTime(timezone=True), nullable=False)

    def __init__(self, id, market_id, high, low, price, volume, time):
        self.id = id
        self.market_id = market_id
        self.high = high
        self.low = low
        self.price = price
        self.volume = volume
        self.time = time

    def save(self):
        db.session.add(self)
        db.session.commit()

    @staticmethod
    def save_table(df):
        df.to_sql('tickers',
                  con=db.engine,
                  if_exists='append',
                  index=False)
        print('table successfully updated')

    @staticmethod
    def get_with_market_id(market_id):
        return pd.read_sql(Tickers.query.filter_by(market_id=market_id).statement,
                           con=db.engine)

    @staticmethod
    def get_with_market_id_all(market_id):
        return pd.read_sql(Tickers.query.filter_by(market_id=market_id).statement,
                           con=db.engine)

    @staticmethod
    def get_coins_with_ids(ids):
        return pd.read_sql(Tickers.query.filter(Tickers.market_id.in_(ids)).statement,
                           con=db.engine)[['market_id', 'close', 'time']]


    @staticmethod
    def get_by_id(id):
        return Tickers.query.get(id)

    @staticmethod
    def get_all():
        return pd.read_sql_table('tickers',
                                 db.engine)


def test_connection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

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
        db.create_all()
        print('Tables for {} successfully created.'. format(cnf['database']))
    except (Exception, psycopg2.DatabaseError) as e:
        print('{} occurred during tables creation for {}'.format(e, cnf['databse']))


def delete_tables():
    try:
        db.drop_all()
        print('Tables for {} successfully deleted.'. format(cnf['database']))
    except (Exception, psycopg2.DatabaseError) as e:
        print('{} occurred during tables deletion for {}'.format(e, cnf['database']))


if __name__ == '__main__':
    test_connection()


