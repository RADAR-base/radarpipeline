from typing import List

import pandas as pd
import pyspark.sql.functions as f

from radarpipeline.datalib.abc import Data
from radarpipeline.datatypes import DataType


class RadarVariableData(Data):
    """
    Class for reading data for a single variable of a single user
    """

    _data: DataType

    def __init__(self, data: DataType, df_type: str = "pandas") -> None:
        self._data = data
        self.df_type = df_type
        self._preprocess_data()

    def get_data(self) -> DataType:
        return self._data

    def set_data(self, data: DataType) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.columns)

    def get_data_size(self) -> int:
        if self.df_type == "pandas":
            return len(self._data.index)
        else:
            return int(self._data.count())

    def _preprocess_data(self) -> None:
        """
        Converts all time value columns to datetime format
        """

        if self.df_type == "spark":
            if "value.time" in self.get_data_keys():
                self._data = self._data.withColumn(
                    "value.time", f.to_date(self._data["`value.time`"])
                )
            if "value.timeReceived" in self.get_data_keys():
                self._data = self._data.withColumn(
                    "value.timeReceived", f.to_date(self._data["`value.timeReceived`"])
                )
            if "value.dateTime" in self.get_data_keys():
                self._data = self._data.withColumn(
                    "value.dateTime", f.to_date(self._data["`value.dateTime`"])
                )
        else:
            if "value.time" in self.get_data_keys():
                self._data["value.time"] = pd.to_datetime(
                    self._data["value.time"], unit="s"
                )
            if "value.timeReceived" in self.get_data_keys():
                self._data["value.timeReceived"] = pd.to_datetime(
                    self._data["value.timeReceived"], unit="s"
                )
            if "value.dateTime" in self.get_data_keys():
                self._data["value.dateTime"] = pd.to_datetime(
                    self._data["value.dateTime"], unit="s"
                )
