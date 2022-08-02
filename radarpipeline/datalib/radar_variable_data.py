from typing import List

import pandas as pd
import pyspark.sql as ps
from pyspark.sql.functions import to_date

from radarpipeline.datalib.data import Data


class RadarVariableData(Data):
    def __init__(self, data: ps.DataFrame) -> None:
        self._data = data
        self._preprocess_data()

    def get_data(self) -> ps.DataFrame:
        return self._data

    def set_data(self, data: ps.DataFrame) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.columns)

    def get_data_size(self) -> int:
        return self._data.count()

    def _get_data_as_pd(self) -> pd.DataFrame:
        return self._data.toPandas()

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
