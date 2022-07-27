from functools import partial
from operator import is_not
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from radarpipeline.datalib.data import Data
from radarpipeline.datalib.radar_user_data import RadarUserData


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

    def get_data_by_key(self, key: str) -> Optional[RadarUserData]:
        if key in self._data:
            return self._data[key]
        else:
            return None

    def get_combined_data(self) -> pd.DataFrame:
        return pd.concat(
            [self._data[key].get_combined_data() for key in self._data.keys()]
        ).reset_index(drop=True)

    def get_combined_data_by_variable(
        self, variable: Union[str, List[str]]
    ) -> List[pd.DataFrame]:
        is_only_one_var = False
        if isinstance(variable, str):
            variable = [variable]
            is_only_one_var = True
        combined_arr = []
        for var in variable:
            variable_data_list = list(
                filter(
                    partial(is_not, None),
                    [self._data[key].get_data_by_key(var) for key in self._data.keys()],
                )
            )
            files_data = [data.get_data() for data in variable_data_list]
            combined_arr.append(
                pd.concat(
                    [
                        file_data[key].get_data()
                        for file_data in files_data
                        for key in file_data.keys()
                    ]
                ).reset_index()
            )
        if is_only_one_var:
            return combined_arr[0]
        return combined_arr

    def get_combined_data_by_user_id(
        self, user_id: Union[str, List[str]]
    ) -> List[pd.DataFrame]:
        if isinstance(user_id, str):
            user_id = [user_id]
        return [
            self._data[user].get_data_by_key(user).get_combined_data()
            for user in user_id
        ]

    # def _preprocess_data(self) -> None:
    #    for key in self._data.keys():
    #        for key2 in self._data[key].keys():
    #            self._data[key][key2]._preprocess_data()
