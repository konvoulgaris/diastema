import pandas as pd
import numpy as np

from scipy.stats import zscore


def replace_outliers_with_mean(df: pd.Series) -> pd.Series:
    """
    Replaces outliers in a Series with the mean value but leaves nulls as is.

    Parameters
    ----------
    df : pd.Series
        The Series that will be modified

    Returns
    -------
    pd.Series
        The resulting Series
    """
    x = df.copy(deep=True)
    y = x.dropna()
    """
    Any z-score greater than 3 or less than -3 is considered to be an outlier. This rule of thumb is based on the
    empirical rule. From this rule we see that almost all of the data (99.7%) should be within three standard
    deviations from the mean.
    
    Source: https://www.ctspedia.org/do/view/CTSpedia/OutLier
    """
    mean = y[(np.abs(zscore(y)) < 3)].mean()
    outliers = y.index[(np.abs(zscore(y)) >= 3)]
    y.loc[outliers] = mean
    x.update(y)
    
    return x
