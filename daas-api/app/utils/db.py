import os
import pymongo

MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))


def get_db_connection(host=MONGO_HOST, port=MONGO_PORT) -> pymongo.MongoClient:
    client = pymongo.MongoClient(host, port)

    try:
        client.server_info()
    except:
        raise Exception("Failed to get Mongo connection!")

    return client
