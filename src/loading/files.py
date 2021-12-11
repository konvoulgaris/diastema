import io
import pandas as pd

def load_file_as_dataframe(file: io.BytesIO, file_type: str) -> pd.DataFrame:
    """
    Tries to read the data of a file, and then tries to create
    a corresponding DataFrame

    Parameters
    ----------
    file : io.BytesIO
        The data of the file
    file_type : str
        The file extension of the original file.

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame
    """
    file.seek(0)
    
    if file_type == "csv":
        return pd.read_csv(file)
    elif file_type == "tsv":
        return pd.read_csv(file, sep='\t')
    elif file_type == "json":
        return pd.read_json(file)
    # elif file_type == "xml":
    #     return pd.read_xml(file)
    # elif file_type == "xls" or file_type == "xlsx":
    #     return pd.read_excel(file)
    else:
        return pd.DataFrame()
