import io
import pandas as pd
import warnings
import magic

ALLOWED_MIME_TYPES = {
    "application/csv": "csv",
    "text/plain": "tsv",
    "application/json": "json",
    "text/xml": "xml",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx"
}


def load_file_as_dataframe(file: io.BytesIO, file_type: str = None) -> pd.DataFrame:
    """
    Tries to read the data of a file, then it attemps to detect it's file type (if required), and then tries to create
    a corresponding DataFrame

    Parameters
    ----------
    file : io.BytesIO
        The data of the file
    file_type : str, optional
        The file extension of the original file.
        This skips file type detection, by default None

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame

    Raises
    ------
    Exception
        "Mime file type recognized as unsupported!", recognized file type is unsupported.
    """
    if file_type:
        if not isinstance(file, io.BytesIO):
            file = io.BytesIO(file)
        
        file.seek(0)
        
        if file_type == "csv":
            return pd.read_csv(file)
        elif file_type == "tsv":
            warnings.warn("Trying to read plain text file as a TSV file. DataFrame results are not guaranteed!")
            
            return pd.read_csv(file, sep='\t')
        elif file_type == "json":
            return pd.read_json(file)
        elif file_type == "xml":
            return pd.read_xml(file)
        elif file_type == "xls" or file_type == "xlsx":
            return pd.read_excel(file)
        else:
            return load_file_as_dataframe(file)
    else:
        warnings.warn("Attempting to recognize file type")
        
        mime = magic.from_buffer(file.read(), mime=True)
        ft = ALLOWED_MIME_TYPES.get(mime)
        
        if ft:
            return load_file_as_dataframe(file, ft)
        else:
            raise Exception(f"Mime file type recognized as unsupported!\n\tMime file type: {mime}")
