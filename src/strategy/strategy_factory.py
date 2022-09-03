from simple_sma_strategy import SimpleMovingAverageStrategy

class StrategyFactory():
    def __init__(self):
        self.__strategy={}

    def register_strategy(self, strategy):
        self.__strategy[strategy.__name__] = strategy

    def get_strategy(self, strategy_name):
        strategy = self.__strategy.get(strategy_name)
        if not strategy:
            raise ValueError("Unable to find {}. Either it is not implemented or strategy class name was defined wrongly!")
        return strategy
