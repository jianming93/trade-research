import logging
import requests

from handlers.handler import Handler


class KucoinHandler(Handler):
    def __init__(self, config_file, token=None):
        super().__init__(config_file, token)

    def pre_connection_setup(self):
        # Kucoin requires generating a token. Can use either public or private
        try:
            response = requests.post(self.config["url"]["rest"] + self.config["token_path"])
            response_json = response.json()
            if response_json["code"] != "200000":
                raise ConnectionError(f"Unable to connect to exchange (Code: {response_json['code']})")
            token = response_json["data"]["token"]
            ws_url = response_json["data"]["instanceServers"][0]["endpoint"]
            ws_uri = ws_url + f"?token={token}"
            return ws_uri
        except Exception as err:
            logging.error(err)

    def pre_subscribe_setup(self):
        symbols = ",".join(self.config['symbols'])
        subscribe_json = {
            "id": 0,
            "type": "subscribe",
            "topic": f"/market/level2:{symbols}",
            "privateChannel": False,
            "response": True
        }
        return [self._dump_json(subscribe_json)]

    def on_message(self, message):
        json_message = self._load_json(message)
        symbol = json_message["topic"].split(":")[-1]
        print(message)
