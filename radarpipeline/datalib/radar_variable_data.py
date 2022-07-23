from typing import Dict, List, Optional, Tuple

import pandas as pd

from radarpipeline.datalib.data import Data
from radarpipeline.datalib.radar_file_data import RadarFileData


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

    def get_data_by_key(self, key: str) -> Optional[RadarFileData]:
        if key in self._data:
            return self._data[key]
        else:
            return None

    def get_combined_data(self) -> pd.DataFrame:
        return pd.concat(
            [self._data[key].get_data() for key in self._data.keys()]
        ).reset_index(drop=True)
