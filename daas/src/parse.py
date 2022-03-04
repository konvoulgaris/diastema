from typing import Tuple


def parse_data_dict(data_dict: dict()) -> Tuple[str, str, str, str, str]:
    minio_input = data_dict.get("minio-input")
    minio_output = data_dict.get("minio-output")
    job_id = str(data_dict.get("job-id"))

    if not (minio_input and minio_output and job_id):
        return (None, None, None, None, None)

    input_bucket = minio_input.split("/")[0]
    input_path = minio_input.partition("/")[-1]
    output_bucket = minio_output.split("/")[0]
    output_path = minio_output.partition("/")[-1]

    return (input_bucket, input_path, output_bucket, output_path, job_id)


def get_max_shrink(data_dict: dict()) -> float:
    max_shrink = data_dict.get("max-shrink")

    if not max_shrink:
        max_shrink = 0.2

    return max_shrink


def parse_file(f: str) -> Tuple[str, str, str]:
    file_directory = f.rsplit("/", 1)[0]
    file_extension = f.rsplit(".", 1)[-1]

    return (f, file_directory, file_extension)
