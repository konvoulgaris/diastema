import pandas as pd
import numpy as np

from .statistics import replace_outliers_with_mean
from .utils import generate_number_not_in_list


def drop_null(df: pd.DataFrame, max_shrink: float = 0.2) -> pd.DataFrame:
    """
    Attemps to clean a DataFrame by dropping null values. The decision to drop the null values is based on a maximum
    allowed shrink percentage. This means that if by dropping the null values the DataFrame shrinks a higher percentage
    than allowed, the original DataFrame will be returned instead.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFarme that will be modified
    max_shrink : float, optional
        The maximum allowed shrink percentage, by default 0.2

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame
    """
    drop_any = df.dropna()

    if 1 - (len(drop_any) / len(df)) <= max_shrink:
        return drop_any

    drop_all = df.dropna(how="all")

    if 1 - (len(drop_all) / len(df)) <= max_shrink:
        return drop_all
    else:
        return df


def handle_object_types(df: pd.DataFrame, column_index: int) -> pd.Series:
    """
    Attempts to clean a string column by labelling the values and respecting the already existing string numbers. This
    means that if a string is a number, the label will be that number.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame that will be modified
    column_index : int
        The index of the string column

    Returns
    -------
    pd.Series
        The resulting DataFrame
    """
    column = df.iloc[:, column_index]
    column.fillna("null", inplace=True)
    uniques = column.unique()
    n_uniques = len(uniques)
    keys = {}
    generated = []

    for u in uniques:
        if u.isdigit():
            keys[u] = int(u)
        else:
            keys[u] = generate_number_not_in_list(generated, n_uniques)

    for k in keys:
        while not k.isdigit() and str(keys[k]) in keys:
            keys[k] = generate_number_not_in_list(generated, n_uniques)

    df.iloc[:, column_index] = column.apply(lambda x: keys[x]).astype(int)

    return df


def handle_int_types(df: pd.DataFrame, column_index: int) -> pd.DataFrame:
    """
    Attemps to clean an int column by replacing null values in a way to keep the final distribution of values in line
    with the original

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame that will be modified
    column_index : int
        The index of the int column

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame
    """ 
    column = df.iloc[:, column_index]
    distribution = column.value_counts(normalize=True)
    nulls = column.isnull()
    column.loc[nulls] = np.random.choice(distribution.index,
                                         size=len(column[nulls]),
                                         p=distribution.values)
    
    df.iloc[:, column_index] = column

    return df


def handle_float_types(df: pd.DataFrame, column_index: int) -> pd.DataFrame:
    """
    Attemps to clean a float column in a DataFrame by replacing null values and outliers with the mean value.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame that will be modified
    column_index : int
        The index of the float column

    Returns
    -------
    pd.DataFrame
        The resulting DataFrame
    """
    column = df.iloc[:, column_index]
    column = replace_outliers_with_mean(column)
    column.fillna(column.mean(), inplace=True)
    df.iloc[:, column_index] = column
    
    return df
