import pandas as pd
import database


def get_coin_data_closing(coin):
    """
    retrieves data for specific coin, based on its name (market-name)
    """
    market_id = database.Markets.get_by_market_name(coin)
    return database.Tickers.get_with_market_id(market_id).rename(columns={'close': coin})


def get_market_data_containing(contains):
    market_ids = database.Markets.get_market_name_contains(contains)
    df_all = pd.DataFrame()

    for index, row in market_ids.iterrows():
        df = database.Tickers.get_with_market_id(row['id'])[['close', 'time']].rename(
            columns={'close': row['market_name']})
        if df_all.empty:
            df_all = df
        else:
            # preserve only relevant data for all coins
            df_all = df_all.merge(df, on=['time'], how='inner')

    return df_all


def get_market_data_by_list(market_names):
    market_ids = database.Markets.get_market_id_with_names(market_names)
    df_all = pd.DataFrame()

    for index, row in market_ids.iterrows():
        df = database.Tickers.get_with_market_id(row['id'])[['close', 'time']].rename(columns={'close': row['market_name']})
        if df_all.empty:
            df_all = df
        else:
            # preserve only relevant data for all coins
            df_all = df_all.merge(df, on=['time'], how='inner')

    return df_all


def get_coin_data_all(market):
    market_id = database.Markets.get_by_market_name(market)
    data = database.Tickers.get_with_market_id(market_id)
    return data[['close', 'volume', 'time']]


if __name__ == '__main__':
    # collect coins with usdt base
    df = get_market_data_containing('USDT')
    df.to_csv('data/x_usdt_coins.csv', index=False)

    # collect btc and eth, both to usdt
    df_btc_eth = get_market_data_by_list(['USDT-BTC', 'USDT-ETH'])
    df_btc_eth.to_csv('data/btc_eth.csv', index=False)

    # collect eth
    df_eth = get_coin_data_all('USDT-ETH')
    df_eth.to_csv('data/eth.csv', index=False)

