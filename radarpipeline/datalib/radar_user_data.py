from .radar_variable_data import RadarVariableData
from . import Data
from typing import List, Tuple, Dict
import pandas as pd

class RadarUserData(Data):
    def __init__(self, data: Dict[str, RadarVariableData]) -> None:
        self._data = data

    def get_data(self) -> Dict[str, RadarVariableData]:
        return self._data

    def set_data(self, data: Dict[str, RadarVariableData]) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return len(self._data)

    def get_data_by_key(self, key: str) -> RadarVariableData:
        return self._data[key]

    def get_combined_data(self) -> pd.DataFrame:
        return pd.concat([self._data[key].get_combined_data() for key in self._data.keys()]).reset_index(drop_index=True)


