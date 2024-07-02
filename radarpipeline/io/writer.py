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
        data_format: str = "csv",
    ) -> None:
        super().__init__(features, output_dir)
        self.num_files = num_files
        self.compression = "gzip" if compress is True else "none"
        self.data_format = data_format

    def write_data(self) -> None:
        for feature_name, feature_df in self.features.items():
            if isinstance(feature_df, dict):
                for key, value in feature_df.items():
                    self.save_dataframe(f"{feature_name}_{key}", value)
            else:
                self.save_dataframe(feature_name, feature_df)

    def save_dataframe(self, feature_name, feature_df):
        folder_name = utils.pascal_to_snake_case(feature_name)
        folder_path = os.path.join(self.output_dir, folder_name)
        try:
            if self.data_format == 'csv':
                feature_df.write.csv(
                    path=folder_path,
                    header=True,
                    sep=constants.CSV_DELIMITER,
                    encoding=constants.ENCODING,
                    compression=self.compression,
                    lineSep=constants.LINESEP,
                )
            elif self.data_format == "parquet":
                feature_df.write.parquet(
                    path=folder_path,
                    compression=self.compression,
                )
            else:
                raise ValueError(
                    f"Invalid data format {self.data_format} specified \
                        for spark writer"
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
        data_format: str = "csv",
    ) -> None:
        super().__init__(features, output_dir)
        self.data_format = data_format
        if data_format == "csv" or data_format == "pickle":
            self.compression = "gzip" if compress is True else "infer"
        elif data_format == "parquet":
            self.compression = "gzip" if compress is True else "snappy"
        else:
            logger.warning("Invalid data format specified. Using default compression")

    def write_data(self) -> None:
        for feature_name, feature_df in self.features.items():
            if isinstance(feature_df, dict):
                for key, value in feature_df.items():
                    self.save_dataframe(f"{feature_name}_{key}", value)
            else:
                self.save_dataframe(feature_name, feature_df)

    def save_dataframe(self, feature_name, feature_df):
        file_path = utils.get_write_file_attr(feature_name,
                                              self.output_dir,
                                              self.data_format,
                                              self.compression)
        try:
            if self.data_format == "csv":
                feature_df.to_csv(
                    file_path,
                    index=False,
                    sep=constants.CSV_DELIMITER,
                    encoding=constants.ENCODING,
                    compression=self.compression,
                )
            elif self.data_format == "pickle":
                feature_df.to_pickle(file_path, compression=self.compression)
            elif self.data_format == "parquet":
                if self.compression == "infer":
                    feature_df.to_parquet(file_path, compression=None)
                else:
                    feature_df.to_parquet(file_path, compression=self.compression)
            else:
                raise ValueError(
                    f"Invalid data format {self.data_format} specified"
                )
        except Exception as e:
            logger.error(
                f"Error writing data to file {file_path}: {e}", exc_info=True
            )
        logger.info(f"Feature {feature_name} data exported to {file_path}")
