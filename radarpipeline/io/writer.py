import logging
import os
from typing import Dict, Optional

import pandas as pd
import pyspark.sql as ps

from radarpipeline.common import constants, utils
from radarpipeline.datatypes.data_types import DataType
from radarpipeline.io.abc import DataWriter

logger = logging.getLogger(__name__)


class SparkDataWriter(DataWriter):
    """
    Writes the data to a local directory using pySpark
    """

    features: Dict[str, ps.DataFrame]
    num_files: Optional[int] = None
    compression: str

    def __init__(
        self,
        features: Dict[str, DataType],
        output_dir: str,
        num_files: Optional[int] = 1,
        compress: bool = False,
    ) -> None:
        super().__init__(features, output_dir)
        self.num_files = num_files
        self.compression = "gzip" if compress is True else "none"

    def write_data(self) -> None:
        for feature_name, feature_df in self.features.items():
            folder_name = utils.pascal_to_snake_case(feature_name)
            folder_path = os.path.join(self.output_dir, folder_name)
            try:
                feature_df.write.csv(
                    path=folder_path,
                    header=True,
                    sep=constants.CSV_DELIMITER,
                    encoding=constants.ENCODING,
                    compression=self.compression,
                    lineSep=constants.LINESEP,
                )
            except Exception as e:
                logger.error(
                    f"Error writing data to file {folder_path}: {e}", exc_info=True
                )
            logger.info(f"Feature {feature_name} data exported to {folder_path}")


class PandasDataWriter(DataWriter):
    """
    Writes the data to a local directory using pandas
    """

    compression: str
    features: Dict[str, pd.DataFrame]

    def __init__(
        self,
        features: Dict[str, DataType],
        output_dir: str,
        compress: bool = False,
    ) -> None:
        super().__init__(features, output_dir)
        self.compression = "gzip" if compress is True else "infer"

    def write_data(self) -> None:
        for feature_name, feature_df in self.features.items():
            file_name = utils.pascal_to_snake_case(feature_name) + ".csv"
            file_name += ".gz" if self.compression == "gzip" else ""
            file_path = os.path.join(self.output_dir, file_name)
            try:
                feature_df.to_csv(
                    file_path,
                    index=False,
                    sep=constants.CSV_DELIMITER,
                    encoding=constants.ENCODING,
                    line_terminator=constants.LINESEP,
                    compression=self.compression,
                )
            except Exception as e:
                logger.error(
                    f"Error writing data to file {file_path}: {e}", exc_info=True
                )
            logger.info(f"Feature {feature_name} data exported to {file_path}")
