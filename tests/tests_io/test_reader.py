import unittest
from radarpipeline.io import SparkCSVDataReader, AvroSchemaReader
import pandas as pd
import radarpipeline
import os
import avro
import json
from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
from radarpipeline.common import constants
from pandas.testing import assert_frame_equal
from numpy.testing import assert_array_equal

from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)
from pyspark.sql.types import StructField, StructType


class TestSparkCSVDataReader(unittest.TestCase):
    def setUp(self):
        mock_config = {"config": {"source_path": "tests/resources/test_data/"}}
        data_list = ['android_phone_step_count']
        self.sparkcsvdatareader = SparkCSVDataReader(mock_config,
                                                     required_data=data_list)
        PANDAS_MOCK_PATH = ("tests/resources/test_data/test_participant/"
                            "android_phone_step_count/0000_11.csv.gz")
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.radar_variable_data = RadarVariableData(self.mock_pandas)
        self.radar_user_data = RadarUserData({"android_phone_step_count":
                                              self.radar_variable_data})
        self.radar_data = RadarData({"test_participant": self.radar_user_data})

    def test_read_data(self):
        spark_data = self.sparkcsvdatareader.read_data()
        assert_array_equal(spark_data.get_data()["test_participant"].get_data()
                           ["android_phone_step_count"].get_data().values,
                           self.mock_pandas.values)
        self.assertTrue(isinstance(spark_data, RadarData))
        self.assertTrue(isinstance(spark_data.get_data()["test_participant"],
                                   RadarUserData))
        self.assertTrue(isinstance(spark_data.get_data()["test_participant"].get_data()
                                   ["android_phone_step_count"], RadarVariableData))

    def tearDown(self):
        self.sparkcsvdatareader.close_spark_session()


class TestSparkCustomConfig(unittest.TestCase):
    def setUp(self):
        mock_config = {"config": {"source_path": "tests/resources/test_data/"}}
        self.spark_config = {
            "spark.executor.instances": "3",
            "spark.memory.offHeap.enabled": False,
            "spark.executor.cores": 2,
            "spark.executor.memory": "5g",
            "spark.driver.memory": "10g",
            "spark.memory.offHeap.size": "10g",
            "spark.driver.maxResultSize": "0",
            "spark.log.level": "ERROR"}
        data_list = ['android_phone_step_count']
        self.sparkcsvdatareader = SparkCSVDataReader(mock_config,
                                                     required_data=data_list,
                                                     spark_config=self.spark_config)

    def test_spark_config(self):
        spark_config_output = dict(self.sparkcsvdatareader.spark.sparkContext.
                                   getConf().getAll())
        for key, value in self.spark_config.items():
            self.assertEqual(spark_config_output[key], str(value))

    def test_spark_config_dict(self):
        spark_config_output = self.sparkcsvdatareader.spark_config
        self.assertDictEqual(self.spark_config, spark_config_output)

    def tearDown(self):
        self.sparkcsvdatareader.close_spark_session()


class TestAvroSchemaReader(unittest.TestCase):
    def setUp(self):
        self.avroschemareader = AvroSchemaReader()
        self.SCHEMA_DIR = ("tests/resources/test_data/test_participant/"
                           "android_phone_step_count")
        self.SCHEMA_TEST_DIR = "tests/resources/test_schemas"
        self.schema_test_schemas_phone_step = "schema-android_phone_step_count.json"
        self.schema_test_schemas_activity_log = ("schema-connect_fitbit_activity_log"
                                                 ".json")

    def test_get_schema_dir_base(self):
        schema_dir_base = self.avroschemareader.get_schema_dir_base(self.SCHEMA_DIR)
        self.assertEqual(schema_dir_base, "android_phone_step_count")

    def test_is_schema_present(self):
        schema_dir_base = self.avroschemareader.get_schema_dir_base(self.SCHEMA_DIR)
        self.assertTrue(self.avroschemareader.is_schema_present(
            self.SCHEMA_DIR,
            schema_dir_base)
        )

    def test_recursive_schema_loader_android_steps(self):
        schema_dict = json.load(
            open(
                os.path.join(self.SCHEMA_TEST_DIR, self.schema_test_schemas_phone_step),
                "r",
                encoding=constants.ENCODING,
            )
        )
        avro_schema = avro.schema.parse(json.dumps(schema_dict))
        schema = self.avroschemareader._recursive_schema_loader(avro_schema)
        expected_schema = {'key.projectId': StringType(),
                           'key.userId': StringType(),
                           'key.sourceId': StringType(),
                           'value.time': DoubleType(),
                           'value.timeReceived': DoubleType(),
                           'value.steps': IntegerType()}
        self.assertDictEqual(schema, expected_schema)

    def test_recursive_schema_loader_activity_log(self):
        schema_dict = json.load(
            open(
                os.path.join(self.SCHEMA_TEST_DIR,
                             self.schema_test_schemas_activity_log),
                "r",
                encoding=constants.ENCODING,
            )
        )
        avro_schema = avro.schema.parse(json.dumps(schema_dict))
        schema = self.avroschemareader._recursive_schema_loader(avro_schema)
        expected_schema = {'key.projectId': StringType(),
                           'key.userId': StringType(),
                           'key.sourceId': StringType(),
                           'value.time': DoubleType(),
                           'value.timeReceived': DoubleType(),
                           'value.timeZoneOffset': IntegerType(),
                           'value.timeLastModified': DoubleType(),
                           'value.duration': FloatType(),
                           'value.durationActive': FloatType(),
                           'value.id': LongType(),
                           'value.name': StringType(),
                           'value.logType': StringType(),
                           'value.type': LongType(),
                           'value.source.id': StringType(),
                           'value.source.name': StringType(),
                           'value.source.type': StringType(),
                           'value.source.url': StringType(),
                           'value.manualDataEntry.distance': BooleanType(),
                           'value.manualDataEntry.energy': BooleanType(),
                           'value.manualDataEntry.steps': BooleanType(),
                           'value.energy': FloatType(),
                           'value.levels.durationSedentary': IntegerType(),
                           'value.levels.durationLightly': IntegerType(),
                           'value.levels.durationFairly': IntegerType(),
                           'value.levels.durationVery': IntegerType(),
                           'value.heartRate.mean': IntegerType(),
                           'value.heartRate.min': IntegerType(),
                           'value.heartRate.max': IntegerType(),
                           'value.heartRate.minFatBurn': IntegerType(),
                           'value.heartRate.minCardio': IntegerType(),
                           'value.heartRate.minPeak': IntegerType(),
                           'value.heartRate.durationOutOfRange': IntegerType(),
                           'value.heartRate.durationFatBurn': IntegerType(),
                           'value.heartRate.durationCardio': IntegerType(),
                           'value.heartRate.durationPeak': IntegerType(),
                           'value.steps': IntegerType(),
                           'value.distance': FloatType(),
                           'value.speed': DoubleType()
                           }
        self.assertDictEqual(schema, expected_schema)

    def test_get_schema_step_count(self):
        schema_name = "android_phone_step_count"
        schema = self.avroschemareader.get_schema(self.SCHEMA_TEST_DIR, schema_name)
        schema_val = schema.get_schema()
        expected_schema = StructType([StructField('key.projectId', StringType(), True),
                                      StructField('key.userId', StringType(), True),
                                      StructField('key.sourceId', StringType(), True),
                                      StructField('value.time', DoubleType(), True),
                                      StructField('value.timeReceived', DoubleType(),
                                                  True),
                                      StructField('value.steps', IntegerType(), True)])
        self.assertEqual(schema_val, expected_schema)

    def test_get_schema_activity_log(self):
        schema_name = "connect_fitbit_activity_log"
        schema = self.avroschemareader.get_schema(self.SCHEMA_TEST_DIR, schema_name)
        schema_val = schema.get_schema()
        expected_schema = StructType([StructField('key.projectId', StringType(), True),
                                      StructField('key.userId', StringType(), True),
                                      StructField('key.sourceId', StringType(), True),
                                      StructField('value.time', DoubleType(), True),
                                      StructField('value.timeReceived',
                                                  DoubleType(), True),
                                      StructField('value.timeZoneOffset',
                                                  IntegerType(), True),
                                      StructField('value.timeLastModified',
                                                  DoubleType(), True),
                                      StructField('value.duration', FloatType(), True),
                                      StructField('value.durationActive',
                                                  FloatType(), True),
                                      StructField('value.id', LongType(), True),
                                      StructField('value.name', StringType(), True),
                                      StructField('value.logType', StringType(), True),
                                      StructField('value.type', LongType(), True),
                                      StructField('value.source.id',
                                                  StringType(), True),
                                      StructField('value.source.name',
                                                  StringType(), True),
                                      StructField('value.source.type',
                                                  StringType(), True),
                                      StructField('value.source.url',
                                                  StringType(), True),
                                      StructField('value.manualDataEntry.distance',
                                                  BooleanType(), True),
                                      StructField('value.manualDataEntry.energy',
                                                  BooleanType(), True),
                                      StructField('value.manualDataEntry.steps',
                                                  BooleanType(), True),
                                      StructField('value.energy', FloatType(), True),
                                      StructField('value.levels.durationSedentary',
                                                  IntegerType(), True),
                                      StructField('value.levels.durationLightly',
                                                  IntegerType(), True),
                                      StructField('value.levels.durationFairly',
                                                  IntegerType(), True),
                                      StructField('value.levels.durationVery',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.mean',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.min',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.max',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.minFatBurn',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.minCardio',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.minPeak',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.durationOutOfRange',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.durationFatBurn',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.durationCardio',
                                                  IntegerType(), True),
                                      StructField('value.heartRate.durationPeak',
                                                  IntegerType(), True),
                                      StructField('value.steps', IntegerType(), True),
                                      StructField('value.distance', FloatType(), True),
                                      StructField('value.speed', DoubleType(), True)])
        self.assertEqual(schema_val, expected_schema)
