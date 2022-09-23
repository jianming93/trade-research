class Portfolio():
    def __init__(self, name, env, auto_balance):
        self.name=name
        self.env=env
        self.auto_balance=auto_balance
        self.strategies={}
        self.allowed_envs=("test", "prod")
        # Validate env
        self.__validate_env()
        
    def __validate_env(self):
        if self.env not in self.allowed_envs:
            raise ValueError("Specified env is not allowed. Allowed envs: {}".format(self.allowed_envs))
        
    def register_strategy(self, strategy_name):
        if strategy_name in self.strategies:
            raise ValueError("Strategy name already exist in portfolio! Please use a different strategy name!")
        self.strategies[strategy_name] = {"weight": 0, "metrics": {}, "universe": {}}
        if self.auto_balance:
            for strategy in self.strategies:
                self.strategies[strategy]["weight"] = 1 / len(self.strategies)
    
    def remove_strategy(self, strategy_name):
        if not strategy_name in self.strategies:
            raise ValueError("Unable to find strategy name: {}. Existing strategy names are: {}".format(strategy_name, self.strategies))
        self.strategies.pop(strategy_name)
        if self.auto_balance:
            for strategy in self.strategies:
                self.strategies[strategy]["weight"] = 1 / len(self.strategies)
        
    def update_weights(self, new_weights):
        if len(self.weights) != len(new_weights):
            raise ValueError("New weights specified are of different length commpared to existing weights. Ensure new weights specified matches the number of strategies present in portfolio!")
        self.weights = new_weights
        
    def update_position(self, position_json):
        strategy_name, universe_name, symbol, position = (
            position_json["strategy_name"],
            position_json["universe_name"],
            position_json["symbol"],
            position_json["position"]
        )
        if universe_name not in self.strategies[strategy_name]["universe"]:
            self.strategies[strategy_name]["universe"][universe_name] = {"metrics": {}, "symbols": {}}
        if symbol not in self.strategies[strategy_name]["universe"][universe_name]["symbols"]:
            self.strategies[strategy_name]["universe"][universe_name]["symbols"][symbol] = {"position": [], "metrics": {}}
        if position is None:
            if self.strategies[strategy_name]["universe"][universe_name]["symbols"][symbol]["position"]:
                position = self.strategies[strategy_name]["universe"][universe_name]["symbols"][symbol]["position"][-1]
            else:
                position = 0
        self.strategies[strategy_name]["universe"][universe_name]["symbols"][symbol]["position"].append(position)