import unittest
import pandas as pd
import numpy as np

from scipy.stats import zscore

from src.dc.clean import drop_null, handle_int_types, handle_float_types, handle_object_types


def is_close(a: float, b: float) -> bool:
    """
    Determine if two numbers are 2 away from one another

    Parameters
    ----------
    a : float
        The first number
    b : float
        The second number

    Returns
    -------
    bool
        True, if 2 away from one another. Else, False.
    """
    x = int(a * 100)
    y = int(b * 100)

    return np.abs(x - y) <= 2


class TestDCClean(unittest.TestCase):
    def test_drop_null(self):
        """
        Test that null values are correctly dropped or ignored
        """
        # Drop "any" test case
        x = pd.DataFrame(np.random.randint(0, 100, size=(1000, 3)))
        x_15 = x.copy() # 15% of values null
        x_15.loc[x.sample(frac=0.15).index, 0] = np.nan
        x_15 = drop_null(x_15)
        
        x_30 = x.copy() # 30% of values null
        x_30.loc[x.sample(frac=0.15).index, 0] = np.nan
        x_30.loc[x.sample(frac=0.15).index, 1] = np.nan
        x_30.loc[x.sample(frac=0.15).index, 2] = np.nan
        
        # Less than 20% modified so it should drop the null values
        self.assertFalse(x_15.isna().values.any())
        # More than 20% modified so it shouldn't drop the null values
        self.assertTrue(x_30.isna().values.any())
        
        # Drop "all" test case
        y = pd.DataFrame(np.random.randint(0, 100, size=(1000, 3)))
        y_15 = y.copy() # 15% of values null
        y_15.loc[y.sample(frac=0.15).index, :] = np.nan
        y_15 = drop_null(x_15)
        
        y_30 = y.copy() # 30% of values null
        y_30.loc[y.sample(frac=0.30).index, :] = np.nan
        y_30 = drop_null(y_30)
        
        # Less than 20% modified so it should drop the null values
        self.assertFalse(y_15.isna().values.any())
        # More than 20% modified so it shouldn't drop the null values
        self.assertTrue(y_30.isna().values.any())


    def test_handle_object_types(self):
        """
        Test that an string column is correctly labelled while respecting already existing numbers.
        """
        x = pd.DataFrame(pd.util.testing.rands_array(16, 300).reshape(100, 3))
        
        nulls = list()
        nulls.append(x.sample(frac=0.15).index.tolist())
        nulls.append(x.sample(frac=0.15).index.tolist())
        nulls.append(x.sample(frac=0.15).index.tolist())
        x.loc[nulls[0], 0] = np.nan
        x.loc[nulls[1], 1] = np.nan
        x.loc[nulls[2], 2] = np.nan
        
        digits = list()
        digits.append(x.sample(frac=0.15).index.tolist())
        digits.append(x.sample(frac=0.15).index.tolist())
        digits.append(x.sample(frac=0.15).index.tolist())
        x.loc[digits[0], 0] = np.random.randint(0, 100)
        x.loc[digits[1], 1] = np.random.randint(0, 100)
        x.loc[digits[2], 2] = np.random.randint(0, 100)
        
        values = list()
        values.append(x.loc[digits[0], 0])
        values.append(x.loc[digits[1], 1])
        values.append(x.loc[digits[2], 2])
        
        for i in range(3):
            x = handle_object_types(x, i)
            
            # Check nulls
            for j in x.loc[nulls[i], i]:
                self.assertTrue(str(j).isdigit())
                
            # Check digits and their values
            for index, value in x.loc[digits[i], i].iteritems():
                self.assertEqual(value, values[i].loc[index])
    

    def test_handle_int_types(self):
        """
        Test that null values in an int column are correctly replaced in a way the keeps the original distribution of
        non-null values
        """
        x = pd.DataFrame(np.random.randint(0, 10, size=(1000, 3)))
        x.loc[x.sample(frac=0.15).index, 0] = np.nan
        x.loc[x.sample(frac=0.15).index, 1] = np.nan
        x.loc[x.sample(frac=0.15).index, 2] = np.nan
        
        distributions = list()
        distributions.append(x.loc[:, 0].value_counts(normalize=True))
        distributions.append(x.loc[:, 1].value_counts(normalize=True))
        distributions.append(x.loc[:, 2].value_counts(normalize=True))
        x = handle_int_types(x, 0)
        x = handle_int_types(x, 1)
        x = handle_int_types(x, 2)
        distributions.append(x.loc[:, 0].value_counts(normalize=True))
        distributions.append(x.loc[:, 1].value_counts(normalize=True))
        distributions.append(x.loc[:, 2].value_counts(normalize=True))
        
        for i in range(3):
            for j in range(len(distributions[i])):
                self.assertTrue(is_close(distributions[i][j], distributions[i + 3][j]))


    def test_handle_float_types(self):
        """
        Test that null values and outliers in a float column are correctly replaced with the mean value
        """
        x = pd.DataFrame(np.random.uniform(0, 10, size=(1000, 3)))
        
        nulls = list()
        nulls.append(x.sample(frac=0.15).index.tolist())
        nulls.append(x.sample(frac=0.15).index.tolist())
        nulls.append(x.sample(frac=0.15).index.tolist())
        x.loc[nulls[0], 0] = np.nan
        x.loc[nulls[1], 1] = np.nan
        x.loc[nulls[2], 2] = np.nan

        for i in range(3):
            y = x.loc[:, i].dropna()
            mean = y[(np.abs(zscore(y)) < 3)].mean()
            
            # Append outlier indices to null index list (saves time)
            nulls[i] += y[(np.abs(zscore(y)) >= 3)].index.tolist()
            
            x = handle_float_types(x, i)
            
            for j in x.loc[nulls[i], i]:
                self.assertAlmostEqual(j, mean)


if __name__ == "__main__":
    unittest.main()
