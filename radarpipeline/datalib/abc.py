from abc import ABC, abstractmethod
from typing import Any, List


class Data(ABC):
    """
    Abstract class for data.
    """

    _data: Any
    df_type: str

    def __init__(self) -> None:
        pass

    @abstractmethod
    def get_data(self) -> Any:
        pass

    @abstractmethod
    def set_data(self, data: Any) -> None:
        pass

    @abstractmethod
    def get_data_keys(self) -> List[str]:
        pass

    @abstractmethod
    def get_data_size(self) -> int:
        pass

    def _preprocess_data(self) -> None:
        """
        Preprocess the data
        """
        for key in self.get_data_keys():
            self._data[key]._preprocess_data()
