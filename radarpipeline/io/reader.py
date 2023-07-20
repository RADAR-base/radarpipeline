import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

import pyspark.sql as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from radarpipeline.common import constants
from radarpipeline.datalib import RadarData, RadarUserData, RadarVariableData
from radarpipeline.io.abc import DataReader, SchemaReader

from multiprocessing import Pool
from functools import partial
from datetime import datetime

logger = logging.getLogger(__name__)


class SparkCSVDataReader(DataReader):
    """
    Read CSV data from local directory using pySpark
    """

    def __init__(self, config: Dict, required_data: List[str], df_type: str = "pandas",
                 spark_config: Dict = {}):
        super().__init__(config)
        default_spark_config = {'spark.executor.instances': 6,
                                'spark.driver.memory': '10G',
                                'spark.executor.cores': 4,
                                'spark.executor.memory': '10g',
                                'spark.memory.offHeap.enabled': True,
                                'spark.memory.offHeap.size': '20g',
                                'spark.driver.maxResultSize': '0'}

        self.required_data = required_data
        self.df_type = df_type
        self.source_path = self.config['config'].get("source_path", "")
        self.spark_config = default_spark_config
        if spark_config is not None:
            self.spark_config.update(spark_config)
        self.spark = self._initialize_spark_session()

    def _initialize_spark_session(self) -> ps.SparkSession:
        """
        Initializes and returns a SparkSession

        Returns
        -------
        SparkSession
            A SparkSession object
        """

        """
        Spark configuration documentation:
        https://spark.apache.org/docs/latest/configuration.html

        `spark.executor.instances` is the number of executors to
        launch for an application.

        `spark.executor.cores` is the number of cores to =
        use on each executor.

        `spark.executor.memory` is the amount of memory to
        use per executor process.

        `spark.driver.memory` is the amount of memory to use for the driver process,
        i.e. where SparkContext is initialized, in MiB unless otherwise specified.

        `spark.memory.offHeap.enabled` is to enable off-heap memory allocation

        `spark.memory.offHeap.size` is the absolute amount of memory which can be used
        for off-heap allocation, in bytes unless otherwise specified.

        `spark.driver.maxResultSize` is the limit of total size of serialized results of
        all partitions for each Spark action (e.g. collect) in bytes.
        Should be at least 1M, or 0 for unlimited.
        """
        spark = (
            SparkSession.builder.master("local").appName("radarpipeline")
            .config('spark.executor.instances',
                    self.spark_config['spark.executor.instances'])
            .config('spark.executor.cores',
                    self.spark_config['spark.executor.cores'])
            .config('spark.executor.memory',
                    self.spark_config['spark.executor.memory'])
            .config('spark.driver.memory',
                    self.spark_config['spark.driver.memory'])
            .config('spark.memory.offHeap.enabled',
                    self.spark_config['spark.memory.offHeap.enabled'])
            .config('spark.memory.offHeap.size',
                    self.spark_config['spark.memory.offHeap.size'])
            .config('spark.driver.maxResultSize',
                    self.spark_config['spark.driver.maxResultSize'])
            .getOrCreate()
        )

        # Enable Apache Arrow for optimizations in Spark to Pandas conversion
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Fallback to use non-Arrow conversion in case of errors
        spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        # For further reading:
        # https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark Session created")
        return spark

    def close_spark_session(self):
        self.spark.stop()

    def read_data(self) -> RadarData:
        """
        Reads RADAR data from local CSV files

        Returns
        -------
        RadarData
            A RadarData object containing all the read data
        """

        user_data_dict = {}

        if not isinstance(self.source_path, list):
            self.source_path = [self.source_path]
        for source_path_item in self.source_path:
            for uid in os.listdir(source_path_item):
                # Skip hidden files
                if uid[0] == ".":
                    continue
                logger.info(f"Reading data for user: {uid}")
                variable_data_dict = {}
                for dirname in self.required_data:
                    if dirname not in os.listdir(os.path.join(source_path_item, uid)):
                        continue
                    logger.info(f"Reading data for variable: {dirname}")
                    absolute_dirname = os.path.abspath(
                        os.path.join(source_path_item, uid, dirname)
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
            schema_fields.append(StructField(f"key.{name}", constants.STRING_TYPE))

        for value in value_dict["type"]["fields"]:
            name = value["name"]
            typ = value["type"]
            field_type = self._get_field(typ)
            schema_fields.append(StructField(f"value.{name}", field_type, True))

        schema = StructType(schema_fields)
        return schema

    def _get_field(self, data_type: Union[str, Dict, List]) -> Any:
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

        if type(data_type) is dict:
            spark_data_type = self._get_data_type_from_dict(data_type)
        elif type(data_type) is list:
            spark_data_type = self._get_superior_type_from_list(data_type)
        else:
            spark_data_type = self._get_data_type_from_mapping(data_type)

        return spark_data_type

    def _handle_unknown_data_type(self, data_type: Union[str, Dict, List]) -> Any:
        """
        Handles unknown data types

        Parameters
        ----------
        data_type : Union[str, Dict]
            Data type to handle

        Returns
        -------
        Any
            A Spark data type
        """

        logger.warning(f"Unknown data type: {data_type}. Returning String type.")
        return constants.STRING_TYPE

    def _get_data_type_from_mapping(self, data_type: Union[str, Dict, List]) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        data_type : str
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if data_type in constants.DATA_TYPE_MAPPING:
            spark_data_type = constants.DATA_TYPE_MAPPING[data_type]
        else:
            spark_data_type = self._handle_unknown_data_type(data_type)

        return spark_data_type

    def _get_data_type_from_dict(self, data_type: Dict) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        data_type : Dict
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if "type" in data_type:
            return self._get_field(data_type["type"])
        else:
            return self._handle_unknown_data_type(data_type)

    def _get_superior_type_from_list(self, data_type_list: List[Any]) -> Any:
        """
        Resolves a list data type to a Spark data type

        Parameters
        ----------
        data_type_list : List
            List data type to resolve

        Returns
        -------
        Any
            A Spark data type
        """

        spark_data_type_list = data_type_list.copy()

        if "null" in spark_data_type_list:
            spark_data_type_list.remove("null")

        for index, data_type in enumerate(spark_data_type_list):
            if type(data_type) is dict:
                spark_data_type_list[index] = self._get_data_type_from_dict(data_type)
            elif data_type in constants.DATA_TYPE_MAPPING:
                spark_data_type_list[index] = constants.DATA_TYPE_MAPPING[data_type]
            else:
                spark_data_type_list[index] = self._handle_unknown_data_type(data_type)

        if len(data_type_list) == 0:
            return constants.STRING_TYPE
        elif len(data_type_list) == 1:
            return spark_data_type_list[0]
        else:
            return self._get_superior_spark_type(spark_data_type_list)

    def _get_superior_spark_type(self, spark_data_type_list: List[Any]) -> Any:
        """
        Returns the superior Spark data type from a list of Spark data types

        Parameters
        ----------
        spark_data_type_list : List
            List of Spark data types

        Returns
        -------
        Any
            A Spark data type
        """

        spark_data_type_set = set(spark_data_type_list)

        # Conflicting types are datatypes which upon conversion can
        # lead to a loss of information
        are_conflicting_types = False
        are_integer_types = False
        are_float_types = False
        are_string_types = False
        are_boolean_types = False

        if len(spark_data_type_set.intersection(constants.INTEGER_TYPES)) > 0:
            are_integer_types = True

        if len(spark_data_type_set.intersection(constants.FLOATING_TYPES)) > 0:
            are_float_types = True

        if constants.STRING_TYPE in spark_data_type_set:
            are_string_types = True

        if constants.BOOLEAN_TYPE in spark_data_type_set:
            are_boolean_types = True

        if (
            int(are_integer_types)
            + int(are_float_types)
            + int(are_string_types)
            + int(are_boolean_types)
            > 1
        ):
            are_conflicting_types = True

        if not are_conflicting_types:
            if are_integer_types:
                if constants.LONG_TYPE in spark_data_type_set:
                    return constants.LONG_TYPE
                elif constants.INT_TYPE in spark_data_type_set:
                    return constants.INT_TYPE
                elif constants.SHORT_TYPE in spark_data_type_set:
                    return constants.SHORT_TYPE
                else:
                    return constants.BYTE_TYPE
            elif are_float_types:
                if constants.DOUBLE_TYPE in spark_data_type_set:
                    return constants.DOUBLE_TYPE
                else:
                    return constants.FLOAT_TYPE
            elif are_string_types:
                return constants.STRING_TYPE
            elif are_boolean_types:
                return constants.BOOLEAN_TYPE
        else:
            logger.warning(
                f"Conflicting types: {spark_data_type_list}. Returning String type."
            )
            return constants.STRING_TYPE


class Reader():
    '''
    Class for reading data from a file
    Reader(data_type : str, data_path: str, variables: Union[str, List])
    reader = Reader(...)
    reader.get_data(variables=Union[List, str])
    reader.get_user_data(user_id=..)
    '''
    def __init__(self, data_type: str, data_path: str, variables: Union[str, List]):
        '''
        Parameters : data_type : str, data_path: str, variables: Union[str, List]
        data_type : str
            Type of data to be read
            Only supports csv for now
        data_path : str
            Path to the data directory
        variables : Union[str, List]
            List of variables to be read
        '''
        self.data_type = data_type
        self.data_path = data_path
        # check if variables is a str
        # If so, convert it to a list
        if isinstance(variables, str):
            variables = [variables]
        self.variables = variables
        config_dict = {"local_directory": self.data_path}
        # check if data_type is csv
        if self.data_type == 'csv':
            self.reader_class = SparkCSVDataReader(config_dict, self.variables)
        else:
            raise NotImplementedError("Only csv data type is supported for now")

    def read_data(self):
        self.data = self.reader_class.read_data()

    def get_data(self, variables: Union[List, str]) -> RadarData:
        return self.data.get_combined_data_by_variable(variables)

    def get_user_data(self, user_id: str) -> RadarData:
        return self.data.get_data_by_user_id(user_id)
