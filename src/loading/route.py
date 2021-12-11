import io
import uuid

from flask import Blueprint, request, g

from .files import load_file_as_dataframe

loading = Blueprint("data-loading", __name__)


@loading.route("/", methods=["POST"])
def loading_index():
    # Get input and output paths
    minio_input = request.form.get("minio-input", None)
    minio_output = request.form.get("minio-output", None)
    
    if not minio_input or not minio_output:
        return "Missing keys in form request", 400
    
    input_bucket = minio_input.split("/")[0]
    output_bucket = minio_output.split("/")[0]
    output_path = minio_output.partition("/")[-1]
    
    # Process files in the input bucket
    files = g.minio.list_objects(input_bucket, recursive=True)
    
    for f in files:
        # Get file details
        path = f.object_name
        extension = f.object_name.split(".")[-1]
        data = g.minio.get_object(input_bucket, path).read()
        
        # Load file as DataFrame
        df = load_file_as_dataframe(io.BytesIO(data), extension)
        
        if df.empty:
            continue
        
        df_data = df.to_csv(index=False).encode("utf-8")
        df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
        
        # Write resulting CSV to MinIO
        g.minio.put_object(output_bucket, df_name, io.BytesIO(df_data),
                           len(df_data), content_type="application/csv")

    return "Done"
