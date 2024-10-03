import json
import logging
import os
from glob import glob
import gzip
import re
from typing import Any, Dict, List, Optional, Union
import concurrent.futures

import pyspark.sql as ps
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType
from pyspark.sql.utils import IllegalArgumentException

from radarpipeline.common import constants
from radarpipeline.datalib import RadarData, RadarUserData, RadarVariableData
from radarpipeline.io.abc import DataReader, SchemaReader
from radarpipeline.io.sampler import UserSampler, DataSampler
from radarpipeline.common.utils import get_hash

import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import RecordSchema, Field, PrimitiveSchema, UnionSchema, Schema
from datetime import datetime

from collections import Counter
from functools import reduce, partial

logger = logging.getLogger(__name__)


class Schemas(object):
    def __init__(self, original_schema, original_schema_keys):
        self.original_schema = original_schema
        self.original_schema_hash = self._get_schema_hash(original_schema_keys)
        self.hashdict = {self.original_schema_hash: original_schema}

    def _get_schema_hash(self, schema_keys):
        return get_hash(schema_keys)

    def is_original_schema(self, schema_keys):
        return self._get_schema_hash(schema_keys) == self.original_schema_hash

    def is_schema_present(self, schema_keys):
        return self._get_schema_hash(schema_keys) in self.hashdict

    def is_schema_hash_present(self, schema_hash):
        return schema_hash in self.hashdict

    def get_schema(self, schema_keys=None):
        if schema_keys is None:
            return self.original_schema
        return self.hashdict[self._get_schema_hash(schema_keys)]

    def get_schema_by_hash(self, schema_hash):
        return self.hashdict[schema_hash]

    def add_schema(self, schema_keys, schema):
        schema_hash = self._get_schema_hash(schema_keys)
        self.hashdict[schema_hash] = schema


class Reader():
    '''
    Class for reading data from a file
    Reader(source_type : str, data_path: str, variables: Union[str, List])
    reader = Reader(...)
    reader.get_data(variables=Union[List, str])
    reader.get_user_data(user_id=..)
    '''
    def __init__(self, spark_session: ps.SparkSession,
                 config: Dict, required_data: List[str], df_type: str = "pandas"):
        """_summary_

        Args:
            spark_session (ps.SparkSession): spark session instance
            config (Dict): Configuration data from the config.yaml file
            required_data (List[str]): List of required data
            df_type (str, optional): Type of dataframe format. Defaults to "pandas".
        """
        self.config = config
        self.source_type = self.config["input"]["data_format"]
        self.required_data = required_data
        self.df_type = df_type
        if self.config["configurations"]['user_sampling'] is None:
            self.user_sampler = None
        else:
            self.user_sampler = UserSampler(self.config["configurations"]
                                            ['user_sampling'])
        if self.config["configurations"]['data_sampling'] is None:
            self.data_sampler = None
        else:
            self.data_sampler = DataSampler(self.config["configurations"]
                                            ['data_sampling'])

        if self.source_type in ['csv', 'csv.gz']:
            self.reader_class = SparkCSVDataReader(spark_session, config,
                                                   required_data, df_type,
                                                   self.user_sampler, self.data_sampler)
        else:
            raise NotImplementedError("Only csv data type is supported for now")

    def read_data(self):
        self.data = self.reader_class.read_data()
        return self.data

    def get_data(self, variables: Union[List, str]) -> RadarData:
        return self.data.get_combined_data_by_variable(variables)

    def get_user_data(self, user_id: str) -> RadarData:
        return self.data.get_data_by_user_id(user_id)


class SparkCSVDataReader(DataReader):
    """
    Read CSV data from local directory using pySpark
    """

    def __init__(self, spark_session: ps.SparkSession,
                 config: Dict, required_data: List[str], df_type: str = "pandas",
                 user_sampler: UserSampler = None, data_sampler: DataSampler = None):
        super().__init__(config)
        self.source_formats = {
            # RADAR_OLD: uid/variable/yyyymmdd_hh00.csv.gz
            "RADAR_OLD": re.compile(r"""^[\w-]+/([\w]+)/
                                ([\d_]+.csv.gz|schema-\1.json)""", re.X),
            # RADAR_NEW: uid/variable/yyyymm/yyyymmdd.csv.gz
            "RADAR_NEW": re.compile(r"""^[\w-]+/([\w]+)/
                                    [\d]+/([\d]+.csv.gz$|schema-\1.json$)""", re.X),
            # RADAR_OLD: uid/questionnaire/QuestionaireName/yyyymm/yyyymmdd.csv.gz
            "RADAR_QUES": re.compile(r"""^[\w-]+/([\w]+)/([\w]+)/
                                    [\d]+/([\d]+.csv.gz$|schema-\1.json$)""", re.X)
        }
        self.required_data = required_data
        self.df_type = df_type
        self.source_path = self.config['input']['config'].get("source_path", "")
        self.user_sampler = user_sampler
        self.data_sampler = data_sampler
        self.schema_reader = AvroSchemaReader()
        self.spark = spark_session
        self.unionByName = partial(DataFrame.unionByName, allowMissingColumns=True)

    def _get_source_type(self, source_path):
        """
        Returns the source type of the data
        """
        if source_path[-1] != "/":
            source_path = source_path + "/"
        for x in os.walk(source_path, topdown=False):
            for file in glob(os.path.join(x[0], '*.*')):
                for key, value in self.source_formats.items():
                    file_format = file.replace(source_path, "")
                    if re.match(value, file_format):
                        return key
        raise ValueError("Source path not recognized")

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
            source_type = self._get_source_type(source_path_item)
            if source_type == "RADAR_OLD":
                logger.info("Reading data from old RADAR format")
                radar_data, user_data_dict = self._read_data_from_old_format(
                    source_path_item, user_data_dict)
            elif source_type == "RADAR_NEW" or source_type == "RADAR_QUES":
                logger.info("Reading data from new RADAR format")
                radar_data, user_data_dict = self._read_data_from_new_format(
                    source_path_item, user_data_dict)
        return radar_data

    def _filter_files_by_headers(self, data_files):
        file_dict = {}
        for file in data_files:
            with gzip.open(file, 'rb') as f:
                columns = f.readline().decode("utf-8").split(",")
                f.close()
            column_hash = get_hash(columns)
            if column_hash in file_dict:
                file_dict[column_hash].append(file)
            else:
                file_dict[column_hash] = [file]
        return file_dict

    def _read_variable_data_files(
        self,
        data_files: List[str],
        schema: Schemas = None,
        variable_name: Optional[str] = None
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
        dfs = []
        file_dict = self._filter_files_by_headers(data_files)
        if schema:
            for column_hash in file_dict.keys():
                if schema.is_schema_hash_present(column_hash):
                    df = self.spark.read.load(
                        file_dict[column_hash],
                        format="csv",
                        quote="\"",
                        escape="\"",
                        header=True,
                        schema=schema.get_schema_by_hash(column_hash),
                        enforceSchema="false",
                        encoding=constants.ENCODING,
                    )
                    dfs.append(df)
                else:
                    df = self.spark.read.load(
                        file_dict[column_hash],
                        format="csv",
                        quote="\"",
                        escape="\"",
                        header=True,
                        inferSchema="true",
                        encoding=constants.ENCODING,
                    )
                    inferred_schema = df.schema
                    schema.add_schema(df.columns, inferred_schema)
                    dfs.append(df)
        else:
            for column_hash in file_dict:
                df = self.spark.read.load(
                    file_dict[column_hash],
                    format="csv",
                    header=True,
                    inferSchema="true",
                    encoding=constants.ENCODING,
                )
                dfs.append(df)

        # Spark Join all the dfs
        # check if dfs are empty
        if len(dfs) == 0:
            # creating empty spark df
            df = self.spark.createDataFrame([], schema=schema.get_schema())
            variable_data = RadarVariableData(df, self.df_type)
        else:
            df = reduce(self.unionByName, dfs)
            variable_data = RadarVariableData(df, self.df_type,
                                              data_sampler=self.data_sampler)
        return variable_data

    def _read_data_from_old_format(self, source_path: str, user_data_dict: dict):
        uids = os.listdir(source_path)
        uids = self._remove_hidden_dirs(uids)
        if self.user_sampler is not None:
            uids = self.user_sampler.sample_uids(uids)

        def process_uid(uid):
            logger.info(f"Reading data for user: {uid}")
            variable_data_dict = {}
            for dirname in self.required_data:
                if dirname not in os.listdir(os.path.join(source_path, uid)):
                    continue
                logger.info(f"Reading data for variable: {dirname}")
                absolute_dirname = os.path.abspath(
                    os.path.join(source_path, uid, dirname)
                )
                data_files = [
                    os.path.join(absolute_dirname, f)
                    for f in os.listdir(absolute_dirname)
                    if f.endswith(".csv.gz")
                ]
                schema = None
                schema_dir = absolute_dirname
                schema_dir_base = self.schema_reader.get_schema_dir_base(schema_dir)
                if self.schema_reader.is_schema_present(schema_dir, schema_dir_base):
                    logger.info("Schema found")
                    schema = self.schema_reader.get_schema(schema_dir, schema_dir_base)
                else:
                    logger.info("Schema not found, inferring from data file")
                variable_data = self._read_variable_data_files(data_files, schema,
                                                               schema_dir_base)
                if variable_data.get_data_size() > 0:
                    variable_data_dict[dirname] = variable_data
            user_data_dict[uid] = RadarUserData(variable_data_dict, self.df_type)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_uid, uids)
        radar_data = RadarData(user_data_dict, self.df_type)
        return radar_data, user_data_dict

    def _read_data_from_new_format(self, source_path: str, user_data_dict: dict):
        # RADAR_NEW: uid/variable/yyyymm/yyyymmdd.csv.gz
        uids = os.listdir(source_path)
        uids = self._remove_hidden_dirs(uids)
        if self.user_sampler is not None:
            uids = self.user_sampler.sample_uids(uids)

        def process_uid(uid):
            # Skip hidden files
            if uid[0] == ".":
                return
            logger.info(f"Reading data for user: {uid}")
            variable_data_dict = {}
            for dirname in self.required_data:
                if not os.path.exists(os.path.join(source_path, uid, dirname)):
                    continue
                logger.info(f"Reading data for variable: {dirname}")
                data_files = []
                for date in os.listdir(os.path.join(source_path, uid, dirname)):
                    # Skip hidden files
                    if date[0] == ".":
                        continue
                    absolute_dirname = os.path.abspath(
                        os.path.join(source_path, uid, dirname, date))
                    data_files = data_files + [
                        os.path.join(absolute_dirname, f)
                        for f in os.listdir(absolute_dirname)
                        if f.endswith(".csv.gz")
                    ]
                schema = None
                schema_dir_base = dirname
                schema_dir = absolute_dirname
                if self.schema_reader.is_schema_present(schema_dir, schema_dir_base):
                    logger.info("Schema found")
                    schema = self.schema_reader.get_schema(schema_dir, schema_dir_base)
                else:
                    logger.info("Schema not found, inferring from data file")
                variable_data = self._read_variable_data_files(data_files, schema,
                                                               schema_dir_base)
                if variable_data.get_data_size() > 0:
                    variable_data_dict[dirname] = variable_data
            user_data_dict[uid] = RadarUserData(variable_data_dict, self.df_type)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_uid, uids)
        radar_data = RadarData(user_data_dict, self.df_type)
        return radar_data, user_data_dict

    def _remove_hidden_dirs(self, uids):
        return [uid for uid in uids if uid[0] != "."]


class AvroSchemaReader(SchemaReader):
    """
    Reads schema from local directory
    """

    def __init__(self) -> None:
        self.schema_dict = {}
        super().__init__(self.schema_dict)

    def get_schema_dir_base(self, schema_dir):
        return os.path.basename(schema_dir)

    def is_schema_present(self, schema_dir, schema_dir_base) -> bool:
        """
        Checks if schema is present in local directory

        Returns
        -------
        bool
            True if schema is present, False otherwise
        """
        if "/" in schema_dir_base:
            schema_dir_base = schema_dir_base.split("/")[0]
        schema_file = os.path.join(
            schema_dir, f"schema-{schema_dir_base}.json"
        )

        if os.path.exists(schema_file):
            return True
        return False

    def get_schema(self, schema_dir, schema_dir_base) -> StructType:

        if "/" in schema_dir_base:
            schema_dir_base = schema_dir_base.split("/")[0]
        if schema_dir_base in self.schema_dict:
            return self.schema_dict[schema_dir_base]
        else:
            schema, schema_keys = self._get_schema(schema_dir, schema_dir_base)
            schema_obj = Schemas(schema, schema_keys)
            self.schema_dict[schema_dir_base] = schema_obj
            return schema_obj

    def _get_schema(self, schema_dir, schema_dir_base) -> StructType:
        """
        Reads schema from local directory

        Returns
        -------
        StructType
            A StructType object defining the schema for pySpark
        """

        schema_file = os.path.join(
            schema_dir, f"schema-{schema_dir_base}.json"
        )
        schema_content = json.load(
            open(
                schema_file,
                "r",
                encoding=constants.ENCODING,
            )
        )
        avro_schema = avro.schema.parse(json.dumps(schema_content))
        schema_content_dict = self._recursive_schema_loader(avro_schema)

        schema, schema_keys = self._to_structtype(schema_content_dict)
        return schema, schema_keys

    def _add_new_schema(self, schema_dir_base, schema):
        self.schema_dict[schema_dir_base] = schema

    def _to_structtype(self, schema_dict):
        schema_fields = []
        schema_keys = []
        for key in schema_dict.keys():
            schema_fields.append(StructField(key, schema_dict[key], True))
            schema_keys.append(key)
        schema = StructType(schema_fields)
        return schema, schema_keys

    def _merge_dicts(self, dct1, dct2):
        return {**dct1, **dct2}

    def _recursive_schema_loader(self, record_schema, precursor="", schema_dict={}):
        """_summary_

        Args:
            record_schema (_type_): _description_
            precursor (str, optional): _description_. Defaults to "".
            schema_dict (dict, optional): _description_. Defaults to {}.
        """
        if isinstance(record_schema, RecordSchema):
            for f in record_schema.fields:
                schema_dict = self._merge_dicts(
                    schema_dict,
                    self._recursive_schema_loader(f, precursor, schema_dict)
                )
            return schema_dict
        elif isinstance(record_schema, Field):
            if isinstance(record_schema.type, RecordSchema):
                if precursor == "":
                    updated_precursor = record_schema.name
                else:
                    updated_precursor = precursor + "." + record_schema.name
                for f in record_schema.type.fields:
                    schema_dict = self._merge_dicts(
                        schema_dict,
                        self._recursive_schema_loader(f, updated_precursor, schema_dict)
                    )
                return schema_dict
            elif isinstance(record_schema.type, UnionSchema):
                if precursor == "":
                    updated_precursor = record_schema.name
                else:
                    updated_precursor = precursor + "." + record_schema.name
                is_record_schema_present = any(
                    isinstance(record_schema_ins, RecordSchema)
                    for record_schema_ins in record_schema.type.schemas
                )
                if is_record_schema_present:
                    for schema_instace in record_schema.type.schemas:
                        if isinstance(schema_instace, RecordSchema):
                            schema_dict = self._merge_dicts(
                                schema_dict,
                                self._recursive_schema_loader(
                                    schema_instace, updated_precursor, schema_dict
                                )
                            )
                else:
                    union_schema = self._resolve_union_schema(
                        record_schema.type.schemas
                    )
                    schema_dict[updated_precursor] = union_schema
                return schema_dict
            else:
                if precursor == "":
                    updated_precursor = record_schema.name
                else:
                    updated_precursor = precursor + "." + record_schema.name
                schema_dict = self._merge_dicts(
                    schema_dict,
                    self._recursive_schema_loader(
                        record_schema.type, updated_precursor, schema_dict)
                )
                return schema_dict

        elif isinstance(record_schema, PrimitiveSchema):
            schema_dict[precursor] = self._get_field(record_schema.type)
            return schema_dict
        else:
            return {}

    def _get_field(self, source_type: Union[str, Dict, List]) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        source_type : Union[str, Dict]
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if type(source_type) is dict:
            spark_data_type = self._get_data_type_from_dict(source_type)
        elif type(source_type) is list:
            spark_data_type = self._get_superior_type_from_list(source_type)
        else:
            spark_data_type = self._get_data_type_from_mapping(source_type)

        return spark_data_type

    def _resolve_union_schema(self, union_schemas: List[Schema]):
        list_type = []
        for schema in union_schemas:
            list_type.append(schema.type)
        return self._get_superior_type_from_list(list_type)

    def _handle_unknown_data_type(self, source_type: Union[str, Dict, List]) -> Any:
        """
        Handles unknown data types

        Parameters
        ----------
        source_type : Union[str, Dict]
            Data type to handle

        Returns
        -------
        Any
            A Spark data type
        """

        logger.warning(f"Unknown data type: {source_type}. Returning String type.")
        return constants.STRING_TYPE

    def _get_data_type_from_mapping(self, source_type: Union[str, Dict, List]) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        source_type : str
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if source_type in constants.DATA_TYPE_MAPPING:
            spark_data_type = constants.DATA_TYPE_MAPPING[source_type]
        else:
            spark_data_type = self._handle_unknown_data_type(source_type)

        return spark_data_type

    def _get_data_type_from_dict(self, source_type: Dict) -> Any:
        """
        Returns a Spark data type for a given data type

        Parameters
        ----------
        source_type : Dict
            Data type to convert to a Spark data type

        Returns
        -------
        Any
            A Spark data type
        """

        if "type" in source_type:
            return self._get_field(source_type["type"])
        else:
            return self._handle_unknown_data_type(source_type)

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

        for index, source_type in enumerate(spark_data_type_list):
            if type(source_type) is dict:
                spark_data_type_list[index] = self._get_data_type_from_dict(source_type)
            elif source_type in constants.DATA_TYPE_MAPPING:
                spark_data_type_list[index] = constants.DATA_TYPE_MAPPING[source_type]
            else:
                spark_data_type_list[index] = self._handle_unknown_data_type(
                    source_type)

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
