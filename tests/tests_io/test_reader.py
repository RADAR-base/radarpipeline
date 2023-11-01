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
        self.data_map = constants.DATA_TYPE_MAPPING

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
        expected_schema = {'key.projectId': self.data_map["string"],
                           'key.userId': self.data_map["string"],
                           'key.sourceId': self.data_map["string"],
                           'value.time': self.data_map["double"],
                           'value.timeReceived': self.data_map["double"],
                           'value.steps': self.data_map["int"]}
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
        expected_schema = {'key.projectId': self.data_map["string"],
                           'key.userId': self.data_map["string"],
                           'key.sourceId': self.data_map["string"],
                           'value.time': self.data_map["double"],
                           'value.timeReceived': self.data_map["double"],
                           'value.timeZoneOffset': self.data_map["int"],
                           'value.timeLastModified': self.data_map["double"],
                           'value.duration': self.data_map["float"],
                           'value.durationActive': self.data_map["float"],
                           'value.id': self.data_map["long"],
                           'value.name': self.data_map["string"],
                           'value.logType': self.data_map["string"],
                           'value.type': self.data_map["long"],
                           'value.source.id': self.data_map["string"],
                           'value.source.name': self.data_map["string"],
                           'value.source.type': self.data_map["string"],
                           'value.source.url': self.data_map["string"],
                           'value.manualDataEntry.distance': self.data_map["boolean"],
                           'value.manualDataEntry.energy': self.data_map["boolean"],
                           'value.manualDataEntry.steps': self.data_map["boolean"],
                           'value.energy': self.data_map["float"],
                           'value.levels.durationSedentary': self.data_map["int"],
                           'value.levels.durationLightly': self.data_map["int"],
                           'value.levels.durationFairly': self.data_map["int"],
                           'value.levels.durationVery': self.data_map["int"],
                           'value.heartRate.mean': self.data_map["int"],
                           'value.heartRate.min': self.data_map["int"],
                           'value.heartRate.max': self.data_map["int"],
                           'value.heartRate.minFatBurn': self.data_map["int"],
                           'value.heartRate.minCardio': self.data_map["int"],
                           'value.heartRate.minPeak': self.data_map["int"],
                           'value.heartRate.durationOutOfRange': self.data_map["int"],
                           'value.heartRate.durationFatBurn': self.data_map["int"],
                           'value.heartRate.durationCardio': self.data_map["int"],
                           'value.heartRate.durationPeak': self.data_map["int"],
                           'value.steps': self.data_map["int"],
                           'value.distance': self.data_map["float"],
                           'value.speed': self.data_map["double"]
                           }
        self.assertDictEqual(schema, expected_schema)

    def test_get_schema_step_count(self):
        schema_name = "android_phone_step_count"
        schema = self.avroschemareader.get_schema(self.SCHEMA_TEST_DIR, schema_name)
        schema_val = schema.get_schema()
        expected_schema = StructType([StructField('key.projectId',
                                                  self.data_map["string"], True),
                                      StructField('key.userId',
                                                  self.data_map["string"], True),
                                      StructField('key.sourceId',
                                                  self.data_map["string"], True),
                                      StructField('value.time',
                                                  self.data_map["double"], True),
                                      StructField('value.timeReceived',
                                                  self.data_map["double"], True),
                                      StructField('value.steps',
                                                  self.data_map["int"], True)])
        self.assertEqual(schema_val, expected_schema)

    def test_get_schema_activity_log(self):
        schema_name = "connect_fitbit_activity_log"
        schema = self.avroschemareader.get_schema(self.SCHEMA_TEST_DIR, schema_name)
        schema_val = schema.get_schema()
        expected_schema = StructType([StructField('key.projectId',
                                                  self.data_map["string"], True),
                                      StructField('key.userId',
                                                  self.data_map["string"], True),
                                      StructField('key.sourceId',
                                                  self.data_map["string"], True),
                                      StructField('value.time',
                                                  self.data_map["double"], True),
                                      StructField('value.timeReceived',
                                                  self.data_map["double"], True),
                                      StructField('value.timeZoneOffset',
                                                  self.data_map["int"], True),
                                      StructField('value.timeLastModified',
                                                  self.data_map["double"], True),
                                      StructField('value.duration',
                                                  self.data_map["float"], True),
                                      StructField('value.durationActive',
                                                  self.data_map["float"], True),
                                      StructField('value.id',
                                                  self.data_map["long"], True),
                                      StructField('value.name',
                                                  self.data_map["string"], True),
                                      StructField('value.logType',
                                                  self.data_map["string"], True),
                                      StructField('value.type',
                                                  self.data_map["long"], True),
                                      StructField('value.source.id',
                                                  self.data_map["string"], True),
                                      StructField('value.source.name',
                                                  self.data_map["string"], True),
                                      StructField('value.source.type',
                                                  self.data_map["string"], True),
                                      StructField('value.source.url',
                                                  self.data_map["string"], True),
                                      StructField('value.manualDataEntry.distance',
                                                  self.data_map["boolean"], True),
                                      StructField('value.manualDataEntry.energy',
                                                  self.data_map["boolean"], True),
                                      StructField('value.manualDataEntry.steps',
                                                  self.data_map["boolean"], True),
                                      StructField('value.energy',
                                                  self.data_map["float"], True),
                                      StructField('value.levels.durationSedentary',
                                                  self.data_map["int"], True),
                                      StructField('value.levels.durationLightly',
                                                  self.data_map["int"], True),
                                      StructField('value.levels.durationFairly',
                                                  self.data_map["int"], True),
                                      StructField('value.levels.durationVery',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.mean',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.min',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.max',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.minFatBurn',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.minCardio',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.minPeak',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.durationOutOfRange',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.durationFatBurn',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.durationCardio',
                                                  self.data_map["int"], True),
                                      StructField('value.heartRate.durationPeak',
                                                  self.data_map["int"], True),
                                      StructField('value.steps',
                                                  self.data_map["int"], True),
                                      StructField('value.distance',
                                                  self.data_map["float"], True),
                                      StructField('value.speed',
                                                  self.data_map["double"], True)])
        self.assertEqual(schema_val, expected_schema)
