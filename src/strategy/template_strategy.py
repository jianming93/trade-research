from base_strategy import BaseStrategy

class TemplateStrategy(BaseStrategy):
    def __init__(self, name, env, preprocessor, postprocessor, max_data_length):
        # Feel free to add any other attributes
        super().__init__(name, env, preprocessor, postprocessor, max_data_length)
        # Define additional attributes here
        

    # Compulsory long_position_rule to define
    def long_position_rule(self, symbol):
        pass
    
    # Compulsory short_position_rule to define
    def short_position_rule(self, symbol):
        pass

    # Any other methods required can just be defined in the class