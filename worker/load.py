import io
import pandas as pd


def load_data_as_dataframe(data: io.BytesIO, extension: str) -> pd.DataFrame:
    if extension == "csv":
        return pd.read_csv(data)
    elif extension == "tsv":
        return pd.read_csv(data)
    else:
        return pd.DataFrame()
