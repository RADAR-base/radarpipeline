import gc
import gzip
import json
import logging
import os
from datetime import datetime
from email import header
from functools import partial, partialmethod
from itertools import repeat
from multiprocessing.pool import Pool
from operator import is_not
from pprint import pprint
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pysftp
import pyspark.sql as ps
from pandas.errors import EmptyDataError
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tqdm import tqdm

from radarpipeline.datalib import RadarData, RadarUserData, RadarVariableData
from radarpipeline.io import DataReader, SchemaReader

logger = logging.getLogger(__name__)


# class SFTPDataReaderCSV(DataReader):
#     def __init__(self, config: Dict, required_data: List[str]) -> None:
#         super().__init__(config)
#         self._create_sftp_credentials()
#         self.required_data = required_data

#     def _create_sftp_credentials(self):
#         if self.config["sftp_password"] is not None:
#             self.sftp_cred = {
#                 "host": self.config["sftp_host"],
#                 "username": self.config["sftp_username"],
#                 "password": self.config["sftp_password"],
#             }
#         elif self.config["sftp_private_key"] is not None:
#             self.sftp_cred = {
#                 "host": self.config["sftp_host"],
#                 "username": self.config["sftp_username"],
#                 "private_key": self.config["sftp_private_key"],
#             }
#         self.source_dir = self.config["sftp_directory"]

#     def _tuples_to_dict(self, tuple_list):
#         """Convert a list of tuples to a dict."""

#         di = dict()
#         for element in tuple_list:
#             if element is not None:
#                 a, b = element
#                 di[a] = b
#         return di

#     def read(self) -> RadarData:
#         with pysftp.Connection(**self.sftp_cred) as sftp:
#             sftp.cwd(self.source_dir)
#             uids = sftp.listdir()
#             user_data_dict = {}
#             for uid in uids:
#                 if uid[0] == ".":
#                     continue
#                 variable_data_arr = {}
#                 for dirname in self.required_data:
#                     file_data_arr = {}
#                     if dirname not in sftp.listdir(f"{uid}/"):
#                         continue
#                     # func = partial(self._read_data_file, sftp, f'{uid}/{dirname}')
#                     data_files = sftp.listdir(f"{uid}/{dirname}/")
#                     # read files parallel using pool
#                     with Pool(8) as p:
#                         file_data_arr = self._tuples_to_dict(
#                             p.starmap(
#                                 self._read_data_file,
#                                 zip(repeat(f"{uid}/{dirname}"), data_files),
#                             )
#                         )
#                     if len(file_data_arr) > 0:
#                         variable_data_arr[dirname] = RadarVariableData(file_data_arr)
#                 user_data_dict[uid] = RadarUserData(variable_data_arr)
#         return RadarData(user_data_dict)

#     def _read_data_file(self, dirs, data_file):
#         with pysftp.Connection(**self.sftp_cred) as sftp:
#             sftp.cwd(self.source_dir)
#             if data_file.split(".")[-1] == "gz":
#                 dt = data_file.split(".")[0]
#                 if len(dt.split("_")) != 2:
#                     return (None, None)
#                 try:
#                     return (
#                         datetime.strptime(dt, "%Y%m%d_%H%M"),
#                         RadarFileData(self._read_csv(sftp, f"{dirs}/{data_file}")),
#                     )
#                 except EmptyDataError:
#                     return (None, None)

#     def _read_csv(self, sftp, path):
#         with sftp.open(path) as f:
#             f.prefetch()
#             gzip_fd = gzip.GzipFile(fileobj=f)
#             df = pd.read_csv(gzip_fd)
#         return df


class SparkCSVDataReader(DataReader):
    """
    Read CSV data from local directory using pySpark
    """

    def __init__(self, config: Dict, required_data: List[str]):
        super().__init__(config)
        self.required_data = required_data
        self.source_path = config.get("local_directory", "")
        self.spark = self._initialize_spark_session()

    def _initialize_spark_session(self) -> ps.SparkSession:
        """
        Initializes and returns a SparkSession

        Returns
        -------
        SparkSession
            A SparkSession object
        """
        spark = SparkSession.builder.master("local").appName("mock").getOrCreate()

        # Enable Apache Arrow for optimizations in Spark to Pandas conversion
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Fallback to use non-Arrow conversion in case of errors
        spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        # For further reading: https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session created")

        return spark

    def read(self) -> RadarData:
        """
        Reads RADAR data from local CSV files

        Returns
        -------
        RadarData
            A RadarData object containing all the read data
        """

        user_data_dict = {}

        for uid in os.listdir(self.source_path):
            # Skip hidden files
            if uid[0] == ".":
                continue

            logger.info(f"Reading data for user: {uid}")

            variable_data_dict = {}

            for dirname in self.required_data:
                if dirname not in os.listdir(os.path.join(self.source_path, uid)):
                    continue

                logger.info(f"Reading data for variable: {dirname}")

                absolute_dirname = os.path.abspath(
                    os.path.join(self.source_path, uid, dirname)
                )

                data_files = [
                    os.path.join(absolute_dirname, f)
                    for f in os.listdir(absolute_dirname)
                    if f.endswith(".csv.gz")
                ]

                schema = None
                schema_reader = AvroSchemaReader(absolute_dirname)

                if schema_reader.is_schema_present():
                    logger.info("Schema found")
                    schema = schema_reader.get_schema()
                else:
                    logger.info("Schema not found, inferring from data file")

                variable_data = self._read_variable_data_files(data_files, schema)

                if variable_data.get_data_size() > 0:
                    variable_data_dict[dirname] = variable_data

            user_data_dict[uid] = RadarUserData(variable_data_dict)

        radar_data = RadarData(user_data_dict)
        return radar_data

    def _read_variable_data_files(
        self,
        data_files: List[str],
        schema: Optional[StructType] = None,
    ) -> RadarVariableData:
        """
        Reads data from a list of data files and returns a RadarVariableData object

        Parameters
        ----------
        data_files : List[str]
            List of data files to read
        schema : Optional[StructType]
            Schema to use for optimized reading

        Returns
        -------
        RadarVariableData
            A RadarVariableData object containing all the read data
        """

        if schema:
            df = self.spark.read.load(
                data_files,
                format="csv",
                header=True,
                schema=schema,
                inferSchema="false",
                encoding="UTF-8",
            )
        else:
            df = self.spark.read.load(
                data_files,
                format="csv",
                header=True,
                inferSchema="true",
                encoding="UTF-8",
            )

        # To print the dataframe stats for cross-checking
        print(df.describe().show())

        variable_data = RadarVariableData(df)

        return variable_data


class AvroSchemaReader(SchemaReader):
    """
    Reads schema from local directory
    """

    data_type_mapping = {
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "string": StringType(),
    }

    def __init__(self, schema_dir: str) -> None:
        super().__init__(schema_dir)

    def is_schema_present(self) -> bool:
        """
        Checks if schema is present in local directory

        Returns
        -------
        bool
            True if schema is present, False otherwise
        """

        schema_dir_base = os.path.basename(self.schema_dir)
        schema_file = os.path.join(self.schema_dir, f"schema-{schema_dir_base}.json")

        if os.path.exists(schema_file):
            self.schema_file = schema_file
            return True

        return False

    def get_schema(self) -> StructType:
        """
        Reads schema from local directory

        Returns
        -------
        StructType
            A StructType object defining the schema for pySpark
        """

        schema_fields = []
        schema_dict = json.load(
            open(os.path.join(self.schema_dir, self.schema_file), "r", encoding="utf-8")
        )

        key_dict = schema_dict["fields"][0]
        value_dict = schema_dict["fields"][1]

        for key in key_dict["type"]["fields"]:
            name = key["name"]
            schema_fields.append(StructField(f"key.{name}", StringType()))

        for value in value_dict["type"]["fields"]:
            name = value["name"]
            typ = value["type"]
            field_type = self._get_field(typ)
            schema_fields.append(StructField(f"value.{name}", field_type, True))

        schema = StructType(schema_fields)
        return schema

    def _get_field(self, data_type: Union[str, Dict]) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        data_type : Union[str, Dict]
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if type(data_type) is str and data_type in self.data_type_mapping:
            return self.data_type_mapping[data_type]
        elif type(data_type) is dict:
            return StringType()
        else:
            raise ValueError(f"Unknown data type: {data_type}")
