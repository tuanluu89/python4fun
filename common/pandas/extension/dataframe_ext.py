from pandas import DataFrame
from typing import Callable


class DataFrameExt:

    def transform(self: DataFrame, fn: Callable, *args, **kwargs) -> DataFrame:
        """
        Support dataframe chaining transformation
        :param fn:
        :param args:
        :param kwargs:
        :return: transformed dataframe
        """
        return fn(self, *args, **kwargs)


# Extend DataFrame with all public methods from DataFrameExt
for func_name in dir(DataFrameExt):
    func = getattr(DataFrameExt, func_name)
    if callable(func) and not func_name.startswith("__"):
        setattr(DataFrame, func_name, func)