from typing import Dict, List, Optional, Union

import pandas as pd

from radarpipeline.common import utils
from radarpipeline.datalib.abc import Data
from radarpipeline.datalib.radar_user_data import RadarUserData
from radarpipeline.datatypes import DataType


class RadarData(Data):
    """
    Class for reading the RADAR data
    """

    _data: Dict[str, RadarUserData]

    def __init__(self, data: Dict[str, RadarUserData], df_type: str = "pandas") -> None:
        self._data = data
        self.df_type = df_type

    def get_data(self) -> Dict[str, RadarUserData]:
        return self._data

    def set_data(self, data: Dict[str, RadarUserData]) -> None:
        self._data = data

    def get_data_type(self) -> str:
        return self.df_type

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return len(self._data.items())

    def _get_data_by_key(self, key: str) -> Optional[RadarUserData]:
        return self._data.get(key, None)

    def _get_all_user_ids(self) -> List[str]:
        """
        Get all user ids in the data of the RADAR data

        Returns
        -------
        List[str]
            The list of all user ids in the data of the RADAR data
        """

        return self.get_data_keys()

    def get_combined_data_by_variable(
        self, variables: Union[str, List[str]],
        return_dict: bool = False
    ) -> Union[DataType, List[DataType]]:
        """
        Returns the combined data of the RADAR data for the given variables

        Parameters
        ----------
        variables : Union[str, List[str]]
            The variable(s) to get the data for

        Returns
        -------
        Union[
            DataType,
            List[DataType]
        ]
            The combined data of the RADAR data for the given variables
        """
        is_only_one_var, variables = self._normalize_variables(variables)
        variable_dict = self._collect_variable_data(variables)
        return self._combine_and_return_data(variable_dict, is_only_one_var,
                                             return_dict)

    def _normalize_variables(self, variables: Union[str, List[str]]):
        is_only_one_var = isinstance(variables, str)
        if is_only_one_var:
            variables = [variables]
        return is_only_one_var, variables

    def _collect_variable_data(self, variables: List[str]) -> Dict[str, List[DataType]]:
        variable_dict = {}
        for user_id in self._get_all_user_ids():
            user_data = self._get_data_by_key(user_id)
            if user_data:
                for var in variables:
                    if var in user_data._get_all_variables():
                        var_data = user_data.get_data_by_variable(var)
                        if var_data:
                            variable_dict.setdefault(var, []).append(
                                var_data.get_data())
        return variable_dict

    def _combine_and_return_data(
        self, variable_dict: Dict[str, List[DataType]],
        is_only_one_var: bool, return_dict: bool
    ) -> Union[DataType, List[DataType]]:
        variable_data_list = []
        variable_data_dict = {}

        for var, data_list in variable_dict.items():
            if data_list:
                combined_df = utils.combine_pyspark_dfs(data_list)
                if self.df_type == "pandas":
                    combined_df = combined_df.toPandas()
                if return_dict:
                    variable_data_dict[var] = combined_df
                else:
                    variable_data_list.append(combined_df)

        if is_only_one_var:
            if return_dict:
                return variable_data_dict
            elif variable_data_list:
                return variable_data_list[0]
            else:
                raise ValueError(
                    f"No data found for the variable {list(variable_dict.keys())}")
        return variable_data_dict if return_dict else variable_data_list

    def get_data_by_user_id(
        self, user_ids: Union[str, List[str]]
    ) -> Union[RadarUserData, List[RadarUserData]]:
        """
        Returns the data of the RADAR data for the given user ids

        Parameters
        ----------
        user_ids : Union[str, List[str]]
            The user id(s) to get the data for

        Returns
        -------
        Union[
            RadarUserData,
            List[RadarUserData]
        ]
            The data of the RADAR data for the given user ids
        """

        is_only_one_var = False
        if isinstance(user_ids, str):
            is_only_one_var = True
            user_ids = [user_ids]

        all_user_ids = self._get_all_user_ids()
        user_data_list = []

        for user_id in user_ids:
            if user_id in all_user_ids:
                user_data = self._get_data_by_key(user_id)
                if user_data is not None:
                    user_data_list.append(user_data)

        if is_only_one_var:
            return user_data_list[0]
        else:
            return user_data_list

    def get_variable_data(
            self, variables: Union[List, str]) -> Union[DataType, List[DataType]]:
        return self.get_combined_data_by_variable(variables)

    def get_user_data(self, user_id: str) -> Union[DataType, List[DataType]]:
        return self.get_data_by_user_id(user_id)
