import abc
from crypto_engine.strategy.common import STRATEGIES


class StrategyHandler(object):
    __metaclass__ = abc.ABCMeta
    """
    StrategyHandler is abstract class for strategy handler objects.
    Main goal of this objects is to provide appropriate framework for
    specific strategy execution.
    Every framework should have knowledge about:
          - taking appropriate datahandler
          - fetching and preparing data for strategy
          - wrapping strategy results
    """
    def __init__(self, strategy_name, data_handler):
        self.data_handler = data_handler
        self.strategy_name = strategy_name
        self.strategy = StrategyHandler._initialize(self.strategy_name)

    def get_name(self):
        return self.strategy_name

    @abc.abstractmethod
    def _get_data(self):
        raise NotImplementedError('prepare_data method must be implemented')

    @abc.abstractmethod
    def execute_strategy(self):
        raise NotImplementedError('execute_strategy method must be implemented')

    @staticmethod
    def _initialize(strategy_name):
        """
        initialises strategy object, can raise Error if name does not match
        """
        try:
            return STRATEGIES[strategy_name]
        except:
            raise ValueError('Strategy name unknown!')

