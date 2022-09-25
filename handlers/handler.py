import abc
import yaml
import json
import logging

class Handler(abc.ABC):
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

    def _dump_json(self, message):
        return json.dumps(message)

    def _load_json(self, message):
        return json.loads(message)

    @abc.abstractmethod
    def on_message(self, message):
        pass

    @abc.abstractmethod
    def pre_connection_setup(self):
        pass

    @abc.abstractmethod
    def pre_subscribe_setup(self):
        pass
