import io
import pandas as pd
import magic
import warnings

ALLOWED_MIME_TYPES = {
    "application/csv": "csv",
    "text/plain": "tsv",
    "application/json": "json",
    "text/xml": "xml",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx"
}

# FIXME: "No columns to parse from file"
def load_file_as_dataframe(file: io.BytesIO, file_type: str = None) -> pd.DataFrame:
    if file_type:
        if file_type == "csv":
            return pd.read_csv(file)
        elif file_type == "tsv":
            warnings.warn("Detected TSV file; Data results of this file type are not guaranteed!")
            return pd.read_csv(file, sep='\t')
        elif file_type == "json":
            return pd.read_json(file)
        elif file_type == "xml":
            return pd.read_xml(file)
        elif file_type == "xls" or file_type == "xlsx":
            return pd.read_excel(file)
        else:
            raise Exception(f"File type is unsupported!\n\tFile type: {file_type}")
    else:
        warnings.warn("Attempting to recognize file type")
        
        mime = magic.from_buffer(file.read(), mime=True)
        ft = ALLOWED_MIME_TYPES.get(mime)
        
        if ft:
            return load_file_as_dataframe(file, ft)
        else:
            raise Exception(f"Mime file type recognized as unsupported!\n\tMime file type: {mime}")

# TODO: Unit testing :)
if __name__ == "__main__":
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.csv", "rb").read()))
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.tsv", "rb").read()))
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.json", "rb").read()))
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.xml", "rb").read()))
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.xls", "rb").read()))
    load_file_as_dataframe(io.BytesIO(open("data/samples/test.xlsx", "rb").read()))
