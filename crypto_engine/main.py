#!/usr/bin/python
from crypto_engine.strategy.common import MOVING_AVERAGE, ARBITRAGE
from time import time
from crypto_engine import handlers
import json

TICKER_STRATEGIES = [ARBITRAGE]
DATABASE_STRATEGIES = [MOVING_AVERAGE]

STRATEGY_HANDLER = {MOVING_AVERAGE: handlers.moving_average_handler.MovingAverageHandler,
                    ARBITRAGE: handlers.arbitrage_handler.ArbitrageHandler}


def websocket_dummy_send(msg):
    """
    mock for websocket functionality
    """
    return


def websocket_dummy_receive():
    """
    mock for websocket functionality
    """
    return None


def unwrap_msg(msg):
    """
    unwrap msgs from websocket to execution format
    TODO: implement
    """
    return None


def wrap_msg(msg):
    """
    wrap msgs to be sent through websocket in expected json format:
    STRATEGY: EXCHANGES: SIGNALS: {BUY: markets, SELL: markets}
    """
    return json.dumps(msg)


def send_msg(strategies_results):
    """
    sends msg if any strategy resulted with opportunity
    """
    if len(strategies_results) > 0:
        websocket_dummy_send(wrap_msg(strategies_results))


def crypto_engine_os():
    """
    main engine function representing real-time system(os) functionality.
    To be called during deployment.

    One OS cycle contains:
    1. websocket listener - to get new strategy demands (json in format:
                                                                strategy_name,
                                                                exchange(optional, if not delivered use all),
                                                                market(optional, if not delivered use all),
    2. split strategies on one-ticker and database dependent

    3. ticker data collection

    4. one-ticker strategies execution

    5. websocket sender - send result of one_tick strategies for speed execution

    6. save ticker data to database

    7. database strategies execution

    8. websocket sender - send result of database dependent strategies

    Strategy result should be reported in json format: STRATEGY: exchange: signals: markets (later add amount
                                                        and maybe risk (and/or some other) indicator)

    """
    # strategies to be executed every cycle, independent of messages got from UI side
    cycle_ticker_strategies = [ARBITRAGE]
    cycle_database_strategies = [MOVING_AVERAGE]

    ticker_data_handler = handlers.data_handler.DataHandler()
    database_handler = handlers.database_handler.DatabaseHandler()

    while True:
        try:
            # 1.
            strategies_to_execute = unwrap_msg(websocket_dummy_receive())

            ticker_strategies = cycle_ticker_strategies
            db_strategies = cycle_database_strategies

            # 2
            if strategies_to_execute:
                ticker_strategies += [strategy for strategy in strategies_to_execute if strategy in TICKER_STRATEGIES]
                db_strategies += [strategy for strategy in strategies_to_execute if strategy in DATABASE_STRATEGIES]
                # handle duplicates as a part of defensive programming (align two sides to prevent them!)
                ticker_strategies = list(set(ticker_strategies))
                db_strategies = list(set(db_strategies))

            # 3
            exchanges_collected = ticker_data_handler.collect_ticker_data()
            # data successfully collected for at least one ticker -> execute strategies
            # 4
            if len(exchanges_collected) > 0:
                strategy_results = []

                for strategy in ticker_strategies:
                    strategy_handler = STRATEGY_HANDLER[strategy](database_handler=ticker_data_handler)
                    strategy_res = strategy_handler.execute_strategy()
                    if strategy_res:
                        strategy_results.append(strategy_res)
                # 5
                send_msg(strategy_results)

                # 6
                database_handler.save_ticker_data(ticker_data_handler.get_data())

            # 7
            strategy_results = []
            for strategy in db_strategies:
                strategy_handler = STRATEGY_HANDLER[strategy](database_handler=database_handler)

                # TODO: unwrap parameters exchange(s) and market(s) from received msg
                strategy_res = strategy_handler.execute_strategy(exchanges=exchanges_collected)
                if strategy_res:
                    strategy_results.append(strategy_res)

            # 8
            send_msg(strategy_results)

        except Exception as e:
            print('Fatal error {} in crypto_engine cycle at {}!!!'.format(e, time()))
            # send message through websocket
            # terminate
            break


if __name__ == '__main__':
    crypto_engine_os()
