import os
import io
import uuid
import pandas as pd

from flask import Flask, request, jsonify
from minio import Minio

from parse import parse_request_form, parse_file
from load import load_data_as_dataframe
from metadata import Metadata, save_metadata
from clean import drop_null, clean_string, clean_number

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 5000))
MINIO_HOST = os.getenv("MINIO_HOST", "0.0.0.0")
MINIO_PORT = int(os.getenv("MINIO_PORT", 9000))
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_PASS", "minioadmin")

app = Flask(__name__)

client = Minio(f"{MINIO_HOST}:{MINIO_PORT}", access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

try:
    client.list_buckets()
except:
    print("Failed to create connection with MinIO!")
    exit(1)
    
print("Created connection with MinIO!")


@app.route("/data-loading", methods=["POST"])
def data_loading():
    input_bucket, input_path, output_bucket, output_path = parse_request_form(request)

    if not input_bucket:
        return "Missing or invalid keys in request form!", 400

    files = client.list_objects(input_bucket, recursive=True)
    exports = list()
    
    metadata = Metadata(f"Dataset", "N/A", output_path, "N/A", 0, 0, 0)

    for f in files:
        file_path, file_directory, file_extension = parse_file(f.object_name)
        
        if file_directory != input_path:
            continue
        
        data = client.get_object(input_bucket, file_path).read()
        data = io.BytesIO(data)
        data.seek(0)
        
        df = load_data_as_dataframe(data, file_extension)
        
        if df.empty:
            print(f"- Skipping: '{input_bucket}/{file_path}'\n  Reason: Unknown file type!")
            continue
        
        df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
        df_data = df.to_csv(index=False).encode("utf-8")
        df_length = len(df_data)
        df_data = io.BytesIO(df_data)
        df_data.seek(0)

        client.put_object(output_bucket, df_name, df_data, df_length, content_type="application/csv")

        exports.append(f"{output_bucket}/{df_name}")
        metadata.samples += df.shape[0]
        metadata.features += df.shape[1]
        metadata.size += df.memory_usage(deep=True).sum()

    save_metadata(metadata)

    return jsonify({"loaded": exports, "metadata": metadata.to_dict()})


@app.route("/data-cleaning", methods=["POST"])
def data_cleaning():
    input_bucket, input_path, output_bucket, output_path = parse_request_form(request)

    if not input_bucket:
        return "Missing or invalid keys in request form!", 400

    max_shrink = float(request.args.get("max-shrink", 0.2))
    
    files = client.list_objects(input_bucket, recursive=True)
    dfs = list()

    for f in files:
        file_path, file_directory, file_extension = parse_file(f.object_name)
        
        if file_directory != input_path:
            continue
        
        data = client.get_object(input_bucket, file_path).read()
        data = io.BytesIO(data)
        data.seek(0)
        
        df = load_data_as_dataframe(data, file_extension)
        
        if df.empty:
            print(f"- Skipping: '{input_bucket}/{file_path}'\n  Reason: Unknown file type!")
            continue
        
        dfs.append(df)
    
    df = pd.concat(dfs).reset_index(drop=True)
    df = drop_null(df, max_shrink)
    df = clean_string(df)
    df = clean_number(df)

    df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
    df_data = df.to_csv(index=False).encode("utf-8")
    df_length = len(df_data)
    df_data = io.BytesIO(df_data)
    df_data.seek(0)

    client.put_object(output_bucket, df_name, df_data, df_length, content_type="application/csv")
    
    return jsonify(f"{output_bucket}/{df_name}")

if __name__ == "__main__":
    app.run(HOST, PORT, True)
