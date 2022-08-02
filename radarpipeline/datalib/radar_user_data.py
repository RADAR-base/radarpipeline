from typing import Dict, List, Optional, Union

import pandas as pd

from radarpipeline.datalib.data import Data
from radarpipeline.datalib.radar_variable_data import RadarVariableData


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

    def _get_data_by_key(self, key: str) -> Optional[RadarVariableData]:
        return self._data.get(key, None)

    def _get_all_variables(self) -> List[str]:
        return self.get_data_keys()

    def get_data_by_variable(
        self, variables: Union[str, List[str]], as_pandas: bool = False
    ) -> Union[List[Dict[str, RadarVariableData]], List[Dict[str, pd.DataFrame]]]:
        if isinstance(variables, str):
            variables = [variables]
        all_variables = self._get_all_variables()
        variable_data_list = []

        for var in variables:
            if var in all_variables:
                var_data = self._get_data_by_key(var)
                if var_data is not None:
                    if as_pandas:
                        variable_data_list.append(var_data._get_data_as_pd())
                    else:
                        variable_data_list.append(var_data)

        return variable_data_list
