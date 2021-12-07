from .radarFileData import RadarFileData
from typing import List, Tuple, Dict
import pandas as pd
from . import Data

class RadarVariableData(Data):
    def __init__(self, data: Dict[str, RadarFileData]) -> None:
        self._data = data
        self._preprocess_data()

    def get_data(self) -> Dict[str, RadarFileData]:
        return self._data

    def set_data(self, data: Dict[str, RadarFileData]) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return len(self._data)

    def get_data_by_key(self, key: str) -> RadarFileData:
        return self._data[key]

    def get_combined_data(self) -> pd.DataFrame:
        return pd.concat([self._data[key].get_data() for key in self._data.keys()]).reset_index(drop=True)
