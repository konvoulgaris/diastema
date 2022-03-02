import os

from flask import Flask
from minio import Minio

# Get environment variables
MINIO_HOST = os.getenv("MINIO_HOST", "0.0.0.0")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

# Create Flask app
app = Flask(__name__)

# Create and check MinIO connection
client = Minio(f"{MINIO_HOST}:{MINIO_PORT}",
               access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

try:
    client.list_buckets()
except:
    print("Failed to create connection with MinIO!")
    exit(1)

print("Created connection with MinIO!")


# Index route
@app.route("/", methods=["GET"])
def index():
    return "Hello", 200


# Entrypoint
if __name__ == "__main__":
    app.run("0.0.0.0", 5000, True)
