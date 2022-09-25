import logging
import requests

from handlers.handler import Handler


class OKXHandler(Handler):
    def __init__(self, config_file, token=None):
        super().__init__(config_file, token)

    def pre_connection_setup(self):
        # Kucoin requires generating a token. Can use either public or private
        try:
            ws_uri = self.config["url"]["ws"]
            return ws_uri
        except Exception as err:
            logging.error(err)

    def pre_subscribe_setup(self):
        subscribe_messages = []
        args_list = []
        for symbol in self.config["symbols"]:
            args_list.append(
                {
                    "channel": "books",
                    "instId": symbol
                }
            )
        subscribe_json = {
            "op": "subscribe",
            "args": args_list
        }
        subscribe_messages.append(self._dump_json(subscribe_json))
        return subscribe_messages

    def on_message(self, message):
        json_message = self._load_json(message)
        print(json_message)
