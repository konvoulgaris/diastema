import unittest
import numpy as np

from src.dc.utils import generate_number_not_in_list


class TestDCUtils(unittest.TestCase):
    def test_generate_number_not_in_list(self):
        """
        Test that generated number is not already in a list
        """
        high = 100
        x = np.random.randint(0, high, size=99)
        y = generate_number_not_in_list(x, high)
        
        self.assertTrue(y not in x)


if __name__ == "__main__":
    unittest.main()
