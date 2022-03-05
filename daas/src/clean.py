import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder
from scipy.stats import zscore


def drop_null(df: pd.DataFrame, max_shrink: float=0.2) -> pd.DataFrame:
    x = df.copy(deep=True)
    drop_any = x.dropna().reset_index(drop=True)

    if 1 - (len(drop_any) / len(x)) <= max_shrink:
        return drop_any

    drop_all = x.dropna(how="all").reset_index(drop=True)

    if 1 - (len(drop_all) / len(x)) <= max_shrink:
        return drop_all
    else:
        return x


def clean_string(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy(deep=True)
    columns = x.select_dtypes(include="object").columns.tolist()
    
    for c in columns:
        temp = x[c]
        temp.fillna("null", inplace=True)
        temp = LabelEncoder().fit_transform(temp)
        x[c] = temp
        
    return x


def clean_number(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy(deep=True)
    columns = x.select_dtypes(include=[ 
        "int16", "int32", "int64", "float16", "float32", "float64"
    ]).columns.tolist()
    
    for c in columns:
        temp = x[c]
        mean = temp.notnull()[np.abs(zscore(temp)) < 3].mean()
        temp.fillna(mean)
        outliers = temp.index[np.abs(zscore(temp)) >= 3]
        temp.loc[outliers] = mean
        x[c] = temp

    return x
