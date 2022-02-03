from minio import Minio

class MinIOException(Exception):
    pass

class MinIO(Minio):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.endpoint = f"{host}:{port}"

        super().__init__(self.endpoint, access_key=self.username, secret_key=self.password, secure=False)

        try:
            self.list_buckets()
        except:
            raise MinIOException("Failed to connect to MinIO")
