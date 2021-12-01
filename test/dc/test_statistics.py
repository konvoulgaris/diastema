import unittest
import pandas as pd
import numpy as np

from scipy.stats import zscore

from src.dc.statistics import replace_outliers_with_mean


class TestDCStatistics(unittest.TestCase):
    def test_replace_outliers_with_mean(self):
        """
        Test that outliers in a DataFrame are replaced with the mean
        """
        x = pd.Series(np.random.randint(0, 100, size=9999), dtype=int)
        x = x.append(pd.Series([123456789])).reset_index(drop=True)
        mean = x[(np.abs(zscore(x)) < 3)].mean()
        x = replace_outliers_with_mean(x)

        self.assertAlmostEqual(x.iloc[9999], mean)


if __name__ == "__main__":
    unittest.main()
