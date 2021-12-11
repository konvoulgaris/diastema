import pandas as pd
import uuid
import io

from flask import Blueprint, request, g

from .clean import drop_null, handle_object_types, handle_int_types, handle_float_types

cleaning = Blueprint("data-cleaning", __name__)


@cleaning.route("/", methods=["POST"])
def cleaning_index():
    # Get input and output paths
    minio_input = request.form.get("minio-input", None)
    minio_output = request.form.get("minio-output", None)
    
    if not minio_input or not minio_output:
        return "Missing keys in form request", 400
    
    # Get parameters
    max_shrink = float(request.args.get("max-shrink", 0.2))

    input_bucket = minio_input.split("/")[0]
    output_bucket = minio_output.split("/")[0]
    output_path = minio_output.partition("/")[-1]
    
    # Process files in the input bucket
    files = g.minio.list_objects(input_bucket, recursive=True)
    df = pd.DataFrame()
    
    for f in files:
        path = f.object_name
        data = g.minio.get_object(input_bucket, path).read()
        data_bytes = io.BytesIO(data)
        data_bytes.seek(0)
        df = df.append(pd.read_csv(data_bytes), ignore_index=True)
        
    df.reset_index(drop=True, inplace=True)
    
    # Begin cleaning pipeline
    df = drop_null(df, max_shrink)
    
    # Process columns according to their data type
    for i, t in enumerate(df.dtypes):
        if t == "object":
            df = handle_object_types(df, i)
        elif t == "int64":
            df = handle_int_types(df, i)
        else:
            df = handle_float_types(df, i)
            
    # Write resulting CSV to MinIO
    df.reset_index(drop=True, inplace=True)
    df_data = df.to_csv(index=False).encode("utf-8")
    df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
    
    g.minio.put_object(output_bucket, df_name, io.BytesIO(df_data),
                       len(df_data), content_type="application/csv")
    
    return "Done"
