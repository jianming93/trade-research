from base_preprocessor import Preprocessor

class KucoinPreprocessor(Preprocessor):
    def preprocess(self, data):
        symbol = data["topic"].split(":")[1].split("_")[0]
        data = {
            "o": float(data["data"]["candles"][1]),
            "c": float(data["data"]["candles"][2]),
            "h": float(data["data"]["candles"][3]),
            "l": float(data["data"]["candles"][4]),
            "v": float(data["data"]["candles"][5]),
        }
        return symbol, data