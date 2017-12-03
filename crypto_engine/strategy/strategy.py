import abc

BUY = 'BUY'
SELL = 'SELL'


class Strategy(object):
    __metaclass__ = abc.ABCMeta
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy objects.

    The goal of a (derived) Strategy object is to generate Signal
    objects for particular market based on the input data
    generated by a DataHandler object.

    Every object must have calculate_signals (for real-time functionality) and calculate_signals_simulation
    for backtesting purposes.
    """
    @abc.abstractmethod
    def calculate_signals(self, data, markets=None):
        raise NotImplementedError('calculate_signals method must be implemented')

    @abc.abstractmethod
    def calculate_signals_simulation(self, data, markets=None):
        raise NotImplementedError('calculate_signals_simulation method must be implemented')

    @abc.abstractmethod
    def buy_indicator(self):
        """
        Indicator function to generate buy signal
        """
        raise NotImplementedError('buy_indicator method must be implemented')

    @abc.abstractmethod
    def sell_indicator(self):
        """
        Indicator function to generate sell signal
        """
        raise NotImplementedError('sell_indicator method must be implemented')



