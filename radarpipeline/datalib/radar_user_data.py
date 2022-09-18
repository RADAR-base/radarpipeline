from typing import Dict, List, Optional, Union

from radarpipeline.datalib.abc import Data
from radarpipeline.datalib.radar_variable_data import RadarVariableData


class RadarUserData(Data):
    """
    Class for reading data of a single user
    """

    _data: Dict[str, RadarVariableData]

    def __init__(
        self, data: Dict[str, RadarVariableData], df_type: str = "pandas"
    ) -> None:
        self._data = data
        self.df_type = df_type

    def get_data(self) -> Dict[str, RadarVariableData]:
        return self._data

    def set_data(self, data: Dict[str, RadarVariableData]) -> None:
        self._data = data

    def get_data_keys(self) -> List[str]:
        return list(self._data.keys())

    def get_data_size(self) -> int:
        return len(self._data.items())

    def _get_data_by_key(self, key: str) -> Optional[RadarVariableData]:
        return self._data.get(key, None)

    def _get_all_variables(self) -> List[str]:
        """
        Get all variables in the data of the user

        Returns
        -------
        List[str]
            The list of all variables in the data of the user
        """

        return self.get_data_keys()

    def get_data_by_variable(
        self, variables: Union[str, List[str]]
    ) -> Union[RadarVariableData, List[RadarVariableData]]:
        """
        Returns the data of the user for the given variables

        Parameters
        ----------
        variables : Union[str, List[str]]
            The variable(s) to get the data for

        Returns
        -------
        Union[
            RadarVariableData,
            List[RadarVariableData],
        ]
            The data of the user for the given variables
        """

        is_only_one_var = False
        if isinstance(variables, str):
            is_only_one_var = True
            variables = [variables]

        all_variables = self._get_all_variables()
        variable_data_list = []

        for var in variables:
            if var in all_variables:
                var_data = self._get_data_by_key(var)
                if var_data is not None:
                    variable_data_list.append(var_data)

        if is_only_one_var:
            return variable_data_list[0]
        else:
            return variable_data_list
