import abc
import yaml
import logging

import websockets

class Connector(abc.ABC):
    def __init__(self, config_file, token=None):
        self.config_file = config_file
        self.token = None
        self.config = self._parse_config()
        self.websocket = None

    def _parse_config(self):
        with open(self.config_file, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logging.error(exc)

    def connect(self):
        ws_uri = self.pre_connect_setup()
        try:
            self.websocket = websockets.connect(ws_uri)
        except Exception as err:
            logging.error(err)


    def subscribe(self):
        subscribe_messages = self.pre_subscribe_setup()
        for subscribe_message in subscribe_messages:
            self.websocket.send(subscribe_message)

    @abc.abstractmethod
    def pre_connect_setup(self):
        pass

    @abc.abstractmethod
    def pre_subscribe_setup(self):
        pass
