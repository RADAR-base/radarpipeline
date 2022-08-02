from typing import Dict, List, Optional, Union

import pandas as pd

from radarpipeline.common.utils import combine_pyspark_dfs
from radarpipeline.datalib.data import Data
from radarpipeline.datalib.radar_user_data import RadarUserData
from radarpipeline.datalib.radar_variable_data import RadarVariableData


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

    def _get_data_by_key(self, key: str) -> Optional[RadarUserData]:
        return self._data.get(key, None)

    def _get_all_user_ids(self):
        return self.get_data_keys()

    def get_combined_data_by_variable(
        self, variables: Union[str, List[str]], as_pandas: bool = False
    ) -> Union[List[Dict[str, RadarVariableData]], List[Dict[str, pd.DataFrame]]]:
        if isinstance(variables, str):
            variables = [variables]
        all_user_ids = self._get_all_user_ids()

        variable_data_list = []
        variable_dict = {}

        for user_id in all_user_ids:
            user_data = self._get_data_by_key(user_id)
            if user_data is not None:
                user_variables = user_data._get_all_variables()
                for var in variables:
                    if var in user_variables:
                        var_data = user_data._get_data_by_key(var)
                        if var_data is not None:
                            variable_dict.get(var, []).append(var_data.get_data())

        for var in variable_dict:
            if len(variable_dict[var]) > 0:
                combined_df = combine_pyspark_dfs(variable_dict[var])
                if as_pandas:
                    combined_df = combined_df.toPandas()
                variable_data_list.append(combined_df)

        return variable_data_list

    def get_data_by_user_id(
        self, user_ids: Union[str, List[str]], as_pandas: bool = False
    ) -> Union[List[RadarUserData], List[Dict[str, Dict[str, pd.DataFrame]]]]:
        if isinstance(user_ids, str):
            user_id = [user_ids]
        all_user_ids = self._get_all_user_ids()
        user_data_list = []

        for user_id in user_ids:
            if user_id in all_user_ids:
                user_data = self._get_data_by_key(user_id)
                if user_data is not None:
                    if as_pandas:
                        user_data_list.append(user_data._get_data_as_pd())
                    else:
                        user_data_list.append(user_data)

        return user_data_list
