import itertools

from base_strategy import BaseStrategy

class SimpleMovingAverageStrategy(BaseStrategy):
    def __init__(self, name, env, preprocessor, postprocessor, sma_long_duration, sma_short_duration, price_type="c"):
        # max_data_length is sma_long_duration
        super().__init__(name, env, preprocessor, postprocessor, sma_long_duration)
        self.sma_short_duration=sma_short_duration
        self.sma_long_duration=sma_long_duration
        self.price_type=price_type
    
    def calculate_sma(self, symbol):
        # Calculate sma for both duration and perform update
        short_sma = sum(itertools.islice(self.symbols[symbol]["data"][self.price_type], self.sma_long_duration - self.sma_short_duration, self.sma_long_duration)) / self.sma_short_duration
        long_sma = sum(self.symbols[symbol]["data"][self.price_type]) / self.sma_long_duration
        return short_sma, long_sma

    def long_position_rule(self, symbol):
        if len(self.symbols[symbol]["data"][self.price_type]) != self.sma_long_duration:
            return None
        short_sma, long_sma = self.calculate_sma(symbol)
        if short_sma > long_sma:
            return 1
        else:
            return None
    
    def short_position_rule(self, symbol):
        if len(self.symbols[symbol]["data"][self.price_type]) != self.sma_long_duration:
            return None
        short_sma, long_sma = self.calculate_sma(symbol)
        if short_sma < long_sma:
            return 0
        else:
            return None