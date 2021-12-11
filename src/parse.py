from flask import Request
from typing import Tuple


def parse_request_form(r: Request) -> Tuple[str, str, str, str]:
    minio_input = r.form.get("minio-input", None)
    minio_output = r.form.get("minio-output", None)
    
    if not (minio_input and minio_output):
        return (None, None, None, None)
    
    input_bucket = minio_input.split("/")[0]
    input_path = minio_input.partition("/")[-1]
    output_bucket = minio_output.split("/")[0]
    output_path = minio_output.partition("/")[-1]
    
    return (input_bucket, input_path, output_bucket, output_path)


def parse_file(f: str) -> Tuple[str, str, str]:
    file_directory = f.rsplit("/", 1)[0]
    file_extension = f.rsplit(".", 1)[-1]

    return (f, file_directory, file_extension)