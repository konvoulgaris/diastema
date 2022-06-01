import io
import pandas as pd


def load_file_as_dataframe(path: str) -> pd.DataFrame:
    extension = path.rpartition(".")[-1]

    with open(path, "rb") as f:
        buffer = io.BytesIO(f.read())
        df = load_data_as_dataframe(buffer, extension)
    
    return df

def load_data_as_dataframe(data: io.BytesIO, extension: str) -> pd.DataFrame:
    if extension == "csv":
        return pd.read_csv(data)
    elif extension == "tsv":
        return pd.read_csv(data)
    else:
        return pd.DataFrame()
