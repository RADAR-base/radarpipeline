from typing import List
import logging
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType

from radarpipeline.datalib.abc import Data
from radarpipeline.common.utils import preprocess_time_data
from radarpipeline.datatypes import DataType

logger = logging.getLogger(__name__)


class RadarVariableData(Data):
    """
    Class for reading data for a single variable of a single user
    """

    _data: DataType

    def __init__(self, data: DataType, df_type: str = "pandas",
                 data_sampler=None) -> None:
        self._data = data
        self.df_type = df_type
        self._preprocess_data()
        if data_sampler is not None:
            self._data = data_sampler.sample_data(self._data)

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
            self._data = preprocess_time_data(self._data)
        except ValueError:
            logger.warning("Unable to convert time columns to datetime format")
