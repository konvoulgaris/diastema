import io
import pandas as pd


def load_data_as_dataframe(data: io.BytesIO, extension: str) -> pd.DataFrame:
    """
    Loads data as a DataFrame

    Parameters
    ----------
    data : io.BytesIO
        The data that will be loaded as a DataFrame
    extension : str
        The file extension of the file the data is from

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame. Empty if failed to load.
    """
    if extension == "csv":
        return pd.read_csv(data)
    elif extension == "tsv":
        return pd.read_csv(data)
    else:
        return pd.DataFrame()
