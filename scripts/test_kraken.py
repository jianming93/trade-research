import asyncio
import websockets
import json

URI = "wss://ws.kraken.com"

async def handler():
    async with websockets.connect(URI) as ws:
        # subscribe
        data = {
            "event": "subscribe",
            "pair": [
                "XBT/USD",
                "XBT/EUR"
            ],
            "subscription": {
                "name": "ticker"
            }
        }
        await ws.send(json.dumps(data))
        # get all messages (not only with `update`)
        async for message in ws:
            loaded_message = json.loads(message)
            if isinstance(loaded_message, dict):
                print(loaded_message)
            else:
                print(loaded_message)

# --- main ---
if __name__=="__main__":
    asyncio.run(handler())
