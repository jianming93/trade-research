import websockets

class MultiConnectionManager():
    def __init__(self):
        self.connection_managers = {}

    def register_connection_manager(self, name, connection_manager):
        if name in self.connection_managers:
            raise ValueError(
                "Connection manager with specified name () found in connection managers! "
                "Please either delete previous manager or register manager under a different name!"
            )
        self.connection_managers[name] = connection_manager

    def delete_connection_manager(self, name):
        if name not in self.connection_managers:
            raise ValueError(
                "Connection manager with specified name () not found in connection managers! "
                "Please check input again!"
            )
        del self.connection_managers[name]