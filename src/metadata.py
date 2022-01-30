import os
import datetime

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
                "name": str(self.name),
              "source": str(self.source),
            "location": str(self.location),
             "usecase": str(self.usecase),
            "features": int(self.features),
             "samples": int(self.samples),
                "size": int(self.size),
          "created_at": str(self.created_at),
          "updated_at": str(self.updated_at)
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
