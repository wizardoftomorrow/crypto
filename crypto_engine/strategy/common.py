from crypto_engine.strategy import moving_average, arbitrage


# Strategies names
MOVING_AVERAGE = 'MOVING_AVERAGE'
ARBITRAGE = 'ARBITRAGE'

STRATEGIES = {MOVING_AVERAGE: moving_average.MovingAverage(),
              ARBITRAGE: arbitrage.Arbitrage()}
