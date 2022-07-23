from typing import List, Union

import pandas as pd
import pyspark.sql as ps
from pyspark.sql.functions import to_date

from radarpipeline.datalib.data import Data


class RadarFileData(Data):
    def __init__(self, data: Union[pd.DataFrame, ps.DataFrame]) -> None:
        self._data = data

    def get_data(self) -> Union[pd.DataFrame, ps.DataFrame]:
        return self._data

    def set_data(self, data: Union[pd.DataFrame, ps.DataFrame]) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return self._data.size

    def _preprocess_data(self) -> None:
        if "value.time" in self._data.columns:
            self._data = self._data.withColumn(
                "`value.time`", to_date(self._data["`value.time`"])
            )
        if "value.timeReceived" in self._data.columns:
            self._data = self._data.withColumn(
                "`value.timeReceived`", to_date(self._data["`value.timeReceived`"])
            )
        if "value.dateTime" in self._data.columns:
            self._data = self._data.withColumn(
                "`value.dateTime`", to_date(self._data["`value.dateTime`"])
            )
