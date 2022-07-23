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
from typing import Dict, List, Tuple, Union

import pandas as pd
import pysftp
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

from radarpipeline.datalib import (
    RadarData,
    RadarFileData,
    RadarUserData,
    RadarVariableData,
)
from radarpipeline.io import DataReader, SchemaReader

logger = logging.getLogger(__name__)


class SFTPDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self._create_sftp_credentials()
        self.required_data = required_data

    def _create_sftp_credentials(self):
        if self.config["sftp_password"] is not None:
            self.sftp_cred = {
                "host": self.config["sftp_host"],
                "username": self.config["sftp_username"],
                "password": self.config["sftp_password"],
            }
        elif self.config["sftp_private_key"] is not None:
            self.sftp_cred = {
                "host": self.config["sftp_host"],
                "username": self.config["sftp_username"],
                "private_key": self.config["sftp_private_key"],
            }
        self.source_dir = self.config["sftp_directory"]

    def _tuples_to_dict(self, tuple_list):
        """Convert a list of tuples to a dict."""

        di = dict()
        for element in tuple_list:
            if element is not None:
                a, b = element
                di[a] = b
        return di

    def read(self) -> RadarData:
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            uids = sftp.listdir()
            user_data_arr = {}
            for uid in uids:
                if uid[0] == ".":
                    continue
                variable_data_arr = {}
                for dirname in self.required_data:
                    file_data_arr = {}
                    if dirname not in sftp.listdir(f"{uid}/"):
                        continue
                    # func = partial(self._read_data_file, sftp, f'{uid}/{dirname}')
                    data_files = sftp.listdir(f"{uid}/{dirname}/")
                    # read files parallel using pool
                    with Pool(8) as p:
                        file_data_arr = self._tuples_to_dict(
                            p.starmap(
                                self._read_data_file,
                                zip(repeat(f"{uid}/{dirname}"), data_files),
                            )
                        )
                    if len(file_data_arr) > 0:
                        variable_data_arr[dirname] = RadarVariableData(file_data_arr)
                user_data_arr[uid] = RadarUserData(variable_data_arr)
        return RadarData(user_data_arr)

    def _read_data_file(self, dirs, data_file):
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            if data_file.split(".")[-1] == "gz":
                dt = data_file.split(".")[0]
                if len(dt.split("_")) != 2:
                    return (None, None)
                try:
                    return (
                        datetime.strptime(dt, "%Y%m%d_%H%M"),
                        RadarFileData(self._read_csv(sftp, f"{dirs}/{data_file}")),
                    )
                except EmptyDataError:
                    return (None, None)

    def _read_csv(self, sftp, path):
        with sftp.open(path) as f:
            f.prefetch()
            gzip_fd = gzip.GzipFile(fileobj=f)
            df = pd.read_csv(gzip_fd)
        return df


class LocalDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]):
        super().__init__(config)
        self.required_data = required_data
        self.source_path = config.get("local_directory", "")

    def read(self) -> RadarData:
        user_data_arr = {}
        # check if source path directory exists
        if not os.path.isdir(self.source_path):
            raise Exception(f"Path does not exist: {self.source_path}")

        for uid in tqdm(os.listdir(self.source_path)):
            if uid[0] == ".":
                continue
            variable_data_arr = {}
            for dirname in self.required_data:
                file_data_arr = {}
                if dirname not in os.listdir(os.path.join(self.source_path, uid)):
                    continue
                data_files = os.listdir(os.path.join(self.source_path, uid, dirname))
                # read files parallel using pool
                with Pool(8) as p:
                    file_data_arr = self._tuples_to_dict(
                        p.starmap(
                            self._read_data_file,
                            zip(
                                repeat(os.path.join(self.source_path, uid, dirname)),
                                data_files,
                            ),
                        )
                    )
                if len(file_data_arr) > 0:
                    variable_data_arr[dirname] = RadarVariableData(file_data_arr)
            user_data_arr[uid] = RadarUserData(variable_data_arr)
        return RadarData(user_data_arr)

    def _read_data_file(self, dirs, data_file):
        if data_file.split(".")[-1] == "gz":
            dt = data_file.split(".")[0]
            if len(dt.split("_")) != 2:
                return (None, None)
            try:
                return (
                    datetime.strptime(dt, "%Y%m%d_%H%M"),
                    RadarFileData(self._read_csv(os.path.join(dirs, data_file))),
                )
            except EmptyDataError:
                return (None, None)

    def _read_csv(self, path):
        df = pd.read_csv(path)
        return df

    def _tuples_to_dict(self, tuple_list):
        di = dict()
        for element in tuple_list:
            if element is not None:
                a, b = element
                di[a] = b
        return di


class SparkCSVDataReader(DataReader):
    def __init__(self, config: Dict, required_data: List[str]):
        super().__init__(config)
        self.required_data = required_data
        self.source_path = config.get("local_directory", "")
        self.spark = self._initialize_spark_session()

    def read(self):
        user_data_arr = {}

        for uid in os.listdir(self.source_path):
            if uid[0] == ".":
                continue

            logger.info(f"Reading data for user: {uid}")

            variable_data_arr = {}

            for dirname in self.required_data:
                if dirname not in os.listdir(os.path.join(self.source_path, uid)):
                    continue

                logger.info(f"Reading data for variable: {dirname}")

                absolute_dirname = os.path.abspath(
                    os.path.join(self.source_path, uid, dirname)
                )

                data_files = [
                    f
                    for f in os.listdir(os.path.join(self.source_path, uid, dirname))
                    if f.endswith(".csv.gz")
                ]

                schema_reader = AvroSchemaReader(absolute_dirname)

                if schema_reader.is_schema_present():
                    schema = schema_reader.get_schema()
                    file_data_array = self._read_data_files(
                        absolute_dirname, data_files, schema
                    )
                else:
                    file_data_array = self._read_data_files(
                        absolute_dirname, data_files
                    )

                if len(file_data_array) > 0:
                    variable_data_arr[dirname] = RadarVariableData(
                        self._tuples_to_dict(file_data_array)
                    )

            user_data_arr[uid] = RadarUserData(variable_data_arr)

        radar_data = RadarData(user_data_arr)
        # print(
        #     radar_data.get_combined_data_by_variable(
        #         ["android_phone_step_count", "android_phone_battery_level"]
        #     )
        # )
        return radar_data

    def _read_data_files(
        self, dir, data_files, schema=None
    ) -> List[Tuple[datetime, RadarFileData]]:
        file_data_array = []
        for data_file in data_files:
            if data_file.endswith(".csv.gz"):
                dt = data_file.split(".")[0]
                if len(dt.split("_")) != 2:
                    continue
                try:
                    file_data = (
                        datetime.strptime(dt, "%Y%m%d_%H%M"),
                        RadarFileData(
                            self._read_csv(os.path.join(dir, data_file), schema)
                        ),
                    )
                    file_data_array.append(file_data)
                except EmptyDataError:
                    continue
        return file_data_array

    def _read_csv(self, data_file, schema=None, as_pd=False):
        if schema:
            df = self.spark.read.load(
                data_file,
                format="csv",
                header=True,
                schema=schema,
                inferSchema="false",
                encoding="UTF-8",
            )
        else:
            df = self.spark.read.load(
                data_file,
                format="csv",
                header=True,
                inferSchema="true",
                encoding="UTF-8",
            )
        if as_pd:
            return df.toPandas()
        return df

    def _tuples_to_dict(self, tuple_list):
        di = dict()
        for element in tuple_list:
            if element is not None:
                a, b = element
                di[a] = b
        return di

    def _initialize_spark_session(self):
        spark = SparkSession.builder.master("local").appName("mock").getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session created")
        return spark


class AvroSchemaReader(SchemaReader):
    def __init__(self, schema_dir: str):
        super().__init__(schema_dir)

    def is_schema_present(self) -> bool:
        dir_files = os.listdir(self.schema_dir)
        schema_file = list(
            filter(lambda x: x.startswith("schema") and x.endswith(".json"), dir_files)
        )

        if len(schema_file) == 1:
            self.schema_file = schema_file[0]
            logger.info("Schema found")
            return True

        logger.info("Schema not found, inferring from data file")
        return False

    def _get_field(self, data_type: Union[str, Dict]):
        if data_type == "boolean":
            return StringType()
        elif data_type == "int":
            return IntegerType()
        elif data_type == "long":
            return LongType()
        elif data_type == "float":
            return FloatType()
        elif data_type == "double":
            return DoubleType()
        elif data_type == "string":
            return StringType()
        elif type(data_type) is dict:
            return StringType()
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def get_schema(self) -> StructType:
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
            schema_fields.append(StructField(f"value.{name}", field_type))

        return StructType(schema_fields)
