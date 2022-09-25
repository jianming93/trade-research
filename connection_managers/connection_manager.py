import abc

import websockets

class ConnectionManager(abc.ABC):
    def __init__(self, handler):
        self.handler = handler

    async def start_connection_loop(self):
        ws_uri = self.handler.pre_connection_setup()
        subscribe_messages = self.handler.pre_subscribe_setup()
        async with websockets.connect(ws_uri) as websocket:
            for subscribe_message in subscribe_messages:
                await websocket.send(subscribe_message)
            await self.start_message_loop(websocket)

    async def start_message_loop(self, websocket):
        while True:
            message = await websocket.recv()
            self.handler.on_message(message)