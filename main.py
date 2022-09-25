import asyncio

from connection_managers.connection_manager import ConnectionManager
from handlers.kucoin_handler import KucoinHandler
from handlers.okx_handler import OKXHandler

KUCOIN_CONFIG_FILEPATH = "configs/handler_kucoin_config.yml"
OKX_CONFIG_FILEPATH = "configs/handler_okx_config.yml"

if __name__ == "__main__":

    # kc_handler = KucoinHandler(KUCOIN_CONFIG_FILEPATH)
    # kc_connection_manager = ConnectionManager(kc_handler)
    okx_handler = OKXHandler(OKX_CONFIG_FILEPATH)
    okx_connection_manager = ConnectionManager(okx_handler)
    # asyncio.run(kc_connection_manager.start_connection_loop())
    asyncio.run(okx_connection_manager.start_connection_loop())