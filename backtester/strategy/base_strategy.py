from collections import deque

class BaseStrategy():
    def __init__(self, name, env, preprocessor, postprocessor, max_data_length=None):
        self.name=name
        self.env=env
        self.preprocessor=preprocessor
        self.postprocessor=postprocessor
        self.max_data_length=max_data_length
        self.symbols={}
        self.allowed_envs=("test", "prod")
        # Run validation on init
        self.__validate_env()
        
    
    def __validate_env(self):
        if self.env not in self.allowed_envs:
            raise ValueError("Specified env is not allowed. Allowed envs: {}".format(self.allowed_envs))
            
    def __init_symbol_data(self):
        if self.max_data_length is not None:
            return {"data": { "o": deque([], self.max_data_length),
                              "h": deque([], self.max_data_length),
                              "l": deque([], self.max_data_length),
                              "c": deque([], self.max_data_length),
                              "v": deque([], self.max_data_length)},
                    "position": 0}
        else:
            return {"data": { "o": deque([]),
                              "h": deque([]),
                              "l": deque([]),
                              "c": deque([]),
                              "v": deque([])},
                    "position": 0}
    
    def run(self, data):
        symbol, processed_data = self.preprocessor.preprocess(data)
        self.update(symbol, processed_data)
        current_position = self.symbols[symbol]["position"]
        new_position = self.execute(symbol, current_position)
        if new_position is not None:
            self.symbols[symbol]["position"] = new_position
        postprocessed_data = self.postprocessor.postprocess(self.name, symbol, new_position)
        return postprocessed_data
    
    def update(self, symbol, data):
        #  Create data structure if needed
        if symbol not in self.symbols:
            # Basic init
            self.symbols[symbol] = self.__init_symbol_data()
            # Additional features init
            additional_features_init = self.__init_additional_features()
            if additional_features_init is not None:
                self.symbols[symbol].update(additional_features_init)
        # Basic data will be ohlcv
        self.symbols[symbol]["data"]["o"].append(data["o"])
        self.symbols[symbol]["data"]["h"].append(data["h"])
        self.symbols[symbol]["data"]["l"].append(data["l"])
        self.symbols[symbol]["data"]["c"].append(data["c"])
        self.symbols[symbol]["data"]["v"].append(data["v"])
        # If need additional features
        additional_features = self.generate_additional_features(data)
        if additional_features is not None:
            for feature in additional_features:
                self.symbols[symbol]["data"][feature].append(additional_features[feature])
        
    def execute(self, symbol, position):
        if position == 1:
            new_position = self.short_position_rule(symbol)
        else:
            new_position = self.long_position_rule(symbol)
        return new_position
    
    def long_position_rule(self, symbol):
        raise NotImplementedError("All classes inheriting from Strategy class need to implement the 'long_position_rule' method!")
    
    def short_position_rule(self, symbol):
        raise NotImplementedError("All classes inheriting from Strategy class need to implement the 'short_position_rule' method!")
    
    # If additional features required to store for time period, use the bottom 2 method to do so
    def __init_additional_features(self):
        """Initialization of additional features
        """
        return None
    
    def generate_additional_features(self, data):
        return None