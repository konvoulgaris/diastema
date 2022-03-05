from pymongo import MongoClient


class Mongo(MongoClient):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.endpoint = f"mongodb://{self.host}:{self.port}"

        super().__init__(self.endpoint)

        try:
            self.server_info()
        except:
            raise Exception("Failed to connect to Mongo")
