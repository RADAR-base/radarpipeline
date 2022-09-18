from typing import Dict

from radarpipeline.datatypes.data_types import DataType
from radarpipeline.io.abc import DataWriter


class SparkDataWriter(DataWriter):
    """
    Writes the data to a local directory using pySpark
    """

    def __init__(self, features: Dict[str, DataType]) -> None:
        super().__init__(features)
