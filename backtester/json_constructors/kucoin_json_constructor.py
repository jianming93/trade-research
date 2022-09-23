class KucoinJsonConstructor():
    def construct_json(self, symbol, index, row, period):
        if period == "1min":
            seconds_to_minus = int(index.timestamp()) - 60
        return {
            "type":"message",
            "topic":"/market/candles:{}_{}".format(symbol, period),
            "subject":"trade.candles.update",
            "data":{
                "symbol":symbol, # symbol
                "candles":[
                    str(seconds_to_minus), # Start time of candle cycle
                    str(row["o"]), # open price
                    str(row["c"]), # close price
                    str(row["h"]), # high price
                    str(row["l"]), # low price
                    str(row["v"]), # Transaction volume
                    "268280.09830877" # Transaction amount (dummy as only kucoin provides this)
                ],
                "time": int(index.timestamp())
            }
        }