import io
import uuid
import pandas as pd

from typing import List

from parse import parse_file
from load import load_data_as_dataframe
from MinIO import MinIO


def join(minio: MinIO, inputs: List[str], column: str, join_type: str, output: str):
    input_buckets = list()
    input_paths = list()

    for i in inputs:
        input_buckets.append(i.split("/")[0])
        input_paths.append(i.partition("/")[-1])

    output_bucket = output.split("/")[0]
    output_path = output.partition("/")[-1]

    dfs = list()

    for i, x in enumerate(input_buckets):
        input_bucket = x
        input_path = input_paths[i]

        files = minio.list_objects(input_bucket, recursive=True)

        for f in files:
            file_path, file_directory, file_extension = parse_file(f.object_name)

            if file_directory != input_path:
                continue

            data = minio.get_object(input_bucket, file_path).read()
            data = io.BytesIO(data)
            data.seek(0)

            df = load_data_as_dataframe(data, file_extension)

            if not df.empty:
                dfs.append(df)

    if len(dfs) == 0:
        raise Exception("no dataframes found")

    left = dfs[0]

    if len(dfs) == 1:
        return left

    for right in dfs[1:]:
        left = pd.merge(left, right, left_on=column, right_on=column, how=join_type)

    left.reset_index(drop=True, inplace=True)

    df_name = f"{output_path}/{uuid.uuid4().hex}.csv"
    df_data = left.to_csv(index=False).encode("utf-8")
    df_length = len(df_data)
    df_data = io.BytesIO(df_data)
    df_data.seek(0)

    minio.put_object(output_bucket, df_name, df_data, df_length, content_type="application/csv")
