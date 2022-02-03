import os
import datetime

class Metadata:
    def __init__(self, name: str, source: str, location: str, usecase: str, features: int, samples: int, size: int, created_at: datetime.datetime=datetime.datetime.utcnow()):
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
        self.name       = data["name"]
        self.source     = data["source"]
        self.location   = data["location"]
        self.usecase    = data["usecase"]
        self.features   = data["features"]
        self.samples    = data["samples"]
        self.size       = data["size"]
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]
