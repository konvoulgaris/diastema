import numpy as np

from typing import List


def generate_number_not_in_list(x: List[int], high: int) -> int:
    """
    Generates a random number that doesn't already exist in a list

    Parameters
    ----------
    x : List[int]
        The list to compare by
    high : int
        The ceiling of numbers to generate

    Returns
    -------
    int
        The resulting random number
    """    
    return np.random.choice([r for r in range(high) if not r in x])
