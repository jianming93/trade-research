from connectors.connector import Connector

class KucoinConnector(Connector):
    def __init__(self, config_file, token=None):
        super().__init__(config_file, token)

    def pre_connect_setup(self):
        ws_uri = self.config["uri"]["ws"]
        return ws_uri

    def pre_subscribe_setup(self):
        pass