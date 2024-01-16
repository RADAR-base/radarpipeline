from typing import List
import logging
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType

from radarpipeline.datalib.abc import Data
from radarpipeline.datatypes import DataType

logger = logging.getLogger(__name__)


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
        return int(self._data.count())

    def _preprocess_data(self) -> None:
        """
        Converts all time value columns to datetime format
        """
        try:
            time_cols = ["value.time", "value.timeReceived", "value.dateTime"]
            for i, col in enumerate(time_cols):
                if col in self._data.columns:
                    self._data = self._data.withColumn(col, self._data[f"`{col}`"]
                                                       .cast(TimestampType()))
                    self._data.withColumn(col, f.from_unixtime(
                        f.unix_timestamp(f"`{col}`")))
        except ValueError:
            logger.warning("Unable to convert time columns to datetime format")
