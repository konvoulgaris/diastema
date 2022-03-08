import uuid
import requests
import os
import io

from MinIO import MinIO

CHUNK_SIZE = 4096


def download(url: str, path="/tmp", name=uuid.uuid4().hex) -> str:
    print(f"Starting {url} download")

    extension = url.split(".")[-1]
    f_name = os.path.join(path, f"{name}.{extension}")

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(f_name, "wb") as f:
                for c in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(c)
    except Exception as e:
        print(e)
        raise Exception("Failed to download!")

    print(f"{f_name} downloaded")

    return f_name


def upload(file_path: str, minio: MinIO, minio_output: str):
    print(f"Starting {file_path} upload")

    output_bucket = minio_output.split("/")[0]
    output_path = minio_output.partition("/")[-1]

    f_name = f"{output_path}/{file_path.rsplit('/', 1)[-1]}"

    try:
        with open(file_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(0)
            data = f.read()
            stream = io.BytesIO(data)
            minio.put_object(output_bucket, f_name, stream, length=size)
    except Exception as e:
        print(e)
        raise Exception("Failed to download!")

    print(f"{f_name} uploaded")
