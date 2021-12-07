from .data import Data
from .radarUserData import RadarUserData
from typing import List, Tuple, Dict, Union
import pandas as pd

class RadarData(Data):
    def __init__(self, data: Dict[str, RadarUserData]) -> None:
        self._data = data

    def get_data(self) -> Dict[str, RadarUserData]:
        return self._data

    def set_data(self, data: Dict[str, RadarUserData]) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return len(self._data)

    def get_data_by_key(self, key: str) -> RadarUserData:
        return self._data[key]

    def get_combined_data(self) -> pd.DataFrame:
        return pd.concat([self._data[key].get_combined_data() for key in self._data.keys()]).reset_index(drop=True)

    def get_combined_data_by_variable(self, variable: Union[str, List[str]]) -> List[pd.DataFrame]:
        if isinstance(variable, str):
            variable = [variable]
        return [pd.concat([self._data[key].get_data_by_key(var).get_data() for key in self._data.keys()]) for var in variable]

    def get_combined_data_by_user_id(self, user_id:Union[str, List[str]]) -> List[pd.DataFrame]:
        if isinstance(user_id, str):
            user_id = [user_id]
        return [self._data.get_data_by_key(user).get_combined_data() for user in user_id]

    #def _preprocess_data(self) -> None:
    #    for key in self._data.keys():
    #        for key2 in self._data[key].keys():
    #            self._data[key][key2]._preprocess_data()
