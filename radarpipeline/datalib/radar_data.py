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
        self, variables: Union[str, List[str]]
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

        is_only_one_var = False
        if isinstance(variables, str):
            is_only_one_var = True
            variables = [variables]

        all_user_ids = self._get_all_user_ids()
        variable_data_list = []
        variable_dict = {}

        # Store the data of the given variables of each user in a dictionary
        for user_id in all_user_ids:
            user_data = self._get_data_by_key(user_id)
            if user_data is not None:
                user_variables = user_data._get_all_variables()
                for var in variables:
                    if var in user_variables:
                        var_data = user_data.get_data_by_variable(var)
                        if var_data is not None:
                            if var not in variable_dict:
                                variable_dict[var] = []
                            variable_dict[var].append(var_data.get_data())

        # Combine the all data for each variable
        for var in variable_dict:
            if len(variable_dict[var]) > 0:
                if self.df_type == "spark":
                    combined_df = utils.combine_pyspark_dfs(variable_dict[var])
                else:
                    combined_df = pd.concat(variable_dict[var], ignore_index=True)
                variable_data_list.append(combined_df)

        if is_only_one_var:
            return variable_data_list[0]
        else:
            return variable_data_list

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
