import imp
from base_postprocessor import Postprocessor

class KucoinPostprocessor(Postprocessor):
    def postprocess(self, strategy_name, symbol, position):
        return {"strategy_name": strategy_name, "symbol": symbol, "position": position}