from . import Data
import pandas as pd
from typing import List, Tuple, Dict

class RadarFileData(Data):
    def __init__(self, data: pd.DataFrame) -> None:
        self._data = data

    def get_data(self) -> pd.DataFrame:
        return self._data

    def set_data(self, data: pd.DataFrame) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return self._data.keys().tolist()

    def get_data_size(self) -> int:
        return self._data.size