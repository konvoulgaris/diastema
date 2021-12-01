import os

from flask import Flask, g
from minio import Minio

from dl.route import dl
from dc.route import dc

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 5000))
MINIO_HOST = os.getenv("MINIO_HOST", "0.0.0.0")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

app = Flask(__name__)
app.register_blueprint(dl, url_prefix="/data-loading")
app.register_blueprint(dc, url_prefix="/data-cleaning")


@app.before_request
def before_request():
    if not "minio" in g:
        # Create and check MinIO connection
        g.minio = Minio(f"{MINIO_HOST}:{MINIO_PORT}", access_key=MINIO_USER,
                        secret_key=MINIO_PASS, secure=False)
        
        try:
            g.minio.list_buckets()
        except:
            print("Failed to create connection with MinIO!")
            exit(1)
        
        print("Created successful connection with MinIO!")


if __name__ == "__main__":
    app.run(HOST, PORT, True)
