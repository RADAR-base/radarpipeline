import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

import pyspark.sql as ps
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

from radarpipeline.common import constants
from radarpipeline.datalib import RadarData, RadarUserData, RadarVariableData
from radarpipeline.io.abc import DataReader, SchemaReader

logger = logging.getLogger(__name__)


class SparkCSVDataReader(DataReader):
    """
    Read CSV data from local directory using pySpark
    """

    def __init__(self, config: Dict, required_data: List[str], df_type: str = "pandas"):
        super().__init__(config)
        self.required_data = required_data
        self.df_type = df_type
        self.source_path = self.config.get("local_directory", "")
        self.spark = self._initialize_spark_session()

    def _initialize_spark_session(self) -> ps.SparkSession:
        """
        Initializes and returns a SparkSession

        Returns
        -------
        SparkSession
            A SparkSession object
        """
        spark = (
            SparkSession.builder.master("local").appName("radarpipeline").getOrCreate()
        )

        # Enable Apache Arrow for optimizations in Spark to Pandas conversion
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Fallback to use non-Arrow conversion in case of errors
        spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        # For further reading: https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session created")

        return spark

    def read_data(self) -> RadarData:
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

            user_data_dict[uid] = RadarUserData(variable_data_dict, self.df_type)

        radar_data = RadarData(user_data_dict, self.df_type)
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
                encoding=constants.ENCODING,
            )
        else:
            df = self.spark.read.load(
                data_files,
                format="csv",
                header=True,
                inferSchema="true",
                encoding=constants.ENCODING,
            )

        if self.df_type == "pandas":
            df = df.toPandas()

        # To print the dataframe stats for cross-checking
        # print(df.show(5))

        variable_data = RadarVariableData(df, self.df_type)

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
        "enum": StringType(),
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
        self.schema_file = os.path.join(
            self.schema_dir, f"schema-{schema_dir_base}.json"
        )

        if os.path.exists(self.schema_file):
            self.schema_file = self.schema_file
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
            open(
                os.path.join(self.schema_dir, self.schema_file),
                "r",
                encoding=constants.ENCODING,
            )
        )

        key_dict = schema_dict["fields"][0]
        value_dict = schema_dict["fields"][1]

        for key in key_dict["type"]["fields"]:
            name = key["name"]
            schema_fields.append(StructField(f"key.{name}", StringType()))

        for value in value_dict["type"]["fields"]:
            name = value["name"]
            typ = value["type"]
            if type(typ) is dict and "type" in typ:
                typ = typ["type"]
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
