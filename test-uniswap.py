import requests
import web3
import asyncio
import websockets


UNISWAP_V2_FACTORY_ADDRESS = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
UNISWAP_V3_FACTORY_ADDRESS = "0x1F98431c8aD98523631AE4a59f267346ea31F984"

UNISWAP_V2_HTTP_URI = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
UNISWAP_V3_HTTP_URI = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

UNISWAP_V2_WSS_URI = "wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
UNISWAP_V3_WSS_URI = "wss://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

query = """{
  factories(first: 5) {
    id
    poolCount
    txCount
    totalVolumeUSD
    }
}"""

async def hello():
    async with websockets.connect(UNISWAP_V3_WSS_URI, subprotocols='graphql-ws') as websocket:
        await websocket.send(query)
        await websocket.recv()


if __name__=="__main__":
    asyncio.run(hello())