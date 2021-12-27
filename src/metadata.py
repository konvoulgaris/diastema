import os
import datetime

from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "0.0.0.0")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))

# Create and test Mongo connection
client = MongoClient(MONGO_HOST, MONGO_PORT)

try:
    client.server_info()
except:
    print("Failed to create MongoDB connection!")
    exit(1)
    
db = client["DaaS"]["Metadata"]

print("Created MongoDB connection!")


class Metadata:
    def __init__(self, name: str, source: str, location: str, usecase: str, features: int, samples: int, size: int, created_at: datetime.datetime=datetime.datetime.utcnow()):
        """
        Parameters
        ----------
        name : str
            The name of the dataset
        source : str
            The source of the dataset
        location : str
            The location of the dataset in MinIO
        usecase : str
            The usecase of the dataset (i.e. classification, regression, clustering)
        features : int
            The number of features in the dataset
        samples : int
            The number of samples in the dataset
        size : int
            The size of the dataset in bytes
        """
        self.name       = name
        self.source     = source
        self.location   = location
        self.usecase    = usecase
        self.features   = features
        self.samples    = samples
        self.size       = size
        self.created_at = created_at
        self.updated_at = created_at
        
        
    def to_dict(self) -> dict():
        """
        Converts the Metadata structure to a dict

        Returns
        -------
        dict
            The resulting dict
        """
        return {
                "name": self.name,
              "source": self.source,
            "location": self.location,
             "usecase": self.usecase,
            "features": int(self.features),
             "samples": int(self.samples),
                "size": int(self.size),
          "created_at": self.created_at,
          "updated_at": self.updated_at
        }
    
    
    def from_dict(self, data: dict):
        """
        Sets Metadata attributes from the data of a dict

        Parameters
        ----------
        data : dict
            The dict where the data will be collected from
        """
        self.name       = data["name"]
        self.source     = data["source"]
        self.location   = data["location"]
        self.usecase    = data["usecase"]
        self.features   = data["features"]
        self.samples    = data["samples"]
        self.size       = data["size"]
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]


def save_metadata(metadata: Metadata):
    """
    Saves metadata to MongoDB

    Parameters
    ----------
    metadata : Metadata
        The metadata that will be saved
    """
    match = db.find_one({"name": metadata.name})
    
    if match:
        metadata.created_at = match["created_at"]
        db.update_one({"_id": match["_id"]}, {"$set": metadata.to_dict()})
    else:
        db.insert_one(metadata.to_dict())
