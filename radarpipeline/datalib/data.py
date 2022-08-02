from abc import ABC, abstractmethod
from typing import Any, List


class Data(ABC):
    _data: Any

    def __init__(self) -> None:
        pass

    @abstractmethod
    def get_data(self) -> Any:
        pass

    @abstractmethod
    def set_data(self, data) -> None:
        pass

    @abstractmethod
    def get_data_keys(self) -> List[str]:
        pass

    @abstractmethod
    def get_data_size(self) -> int:
        pass

    def _get_data_as_pd(self) -> Any:
        data_as_pd = {}
        for key in self._data.keys():
            data_as_pd[key] = self._data[key]._get_data_as_pd()
        return data_as_pd

    def _preprocess_data(self) -> None:
        for key in self._data.keys():
            self._data[key]._preprocess_data()
