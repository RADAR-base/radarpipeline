from typing import Type, TypeVar, Union

import pandas as pd
import pyspark.sql as ps

DataType = Union[pd.DataFrame, ps.DataFrame]
