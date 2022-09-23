import asyncio

from connectors.kucoin_connector import KucoinConnector

KUCOIN_CONFIG_FILEPATH = "configs/connector_kucoin_config.yml"

if __name__ == "__main__":
    kc_connector = KucoinConnector(KUCOIN_CONFIG_FILEPATH)
    kc_connector.connect()
    print(kc_connector.websocket)