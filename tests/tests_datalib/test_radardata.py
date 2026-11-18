from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
import unittest
import os
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType
from pandas.testing import assert_frame_equal
from radarpipeline.project.sparkengine import SparkEngine
from radarpipeline.common.utils import preprocess_time_data


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark_engine = SparkEngine()
        cls.spark = cls.spark_engine.initialize_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark_engine.close_spark_session()

    def preprocess_data(self, data):
        return preprocess_time_data(data)


class TestRadarData(PySparkTestCase):

    def setUp(self):
        MOCK_PATH = ("tests/resources/test_data/test_participant/"
                     "android_phone_step_count/0000_11.csv.gz")
        self.mock_df = self.spark.read.csv(MOCK_PATH,
                                           header=True,
                                           inferSchema=True)
        self.radar_variable_data = RadarVariableData(self.mock_df)
        self.radar_user_data = RadarUserData({"test_variable_data":
                                              self.radar_variable_data})
        self.radar_data = RadarData({"test_user_data": self.radar_user_data})

    def test_get_data(self):
        self.assertDictEqual(self.radar_data.get_data(), {"test_user_data":
                                                          self.radar_user_data})

    def test_get_data_keys(self):
        self.assertEqual(self.radar_data.get_data_keys(), ["test_user_data"])

    def test_get_data_sizes(self):
        self.assertEqual(self.radar_data.get_data_size(), 1)

    def test_get_data_by_user_id(self):
        self.assertEqual(self.radar_data.get_data_by_user_id("test_user_data"),
                         self.radar_user_data)

    def test_get_all_user_ids(self):
        self.assertEqual(self.radar_data._get_all_user_ids(), ["test_user_data"])

    def test_get_combined_data_by_variable(self):
        self.mock_df = self.preprocess_data(self.mock_df)
        self.mock_pandas = self.mock_df.toPandas()
        assert_frame_equal(self.radar_data.get_combined_data_by_variable(
            "test_variable_data"),
            self.mock_pandas)

    def test_combined_data_is_cached(self):
        first = self.radar_data.get_combined_data_by_variable("test_variable_data")
        self.assertIn("test_variable_data", self.radar_data._combined_cache)
        second = self.radar_data.get_combined_data_by_variable("test_variable_data")
        assert_frame_equal(first, second)
        # Each caller gets its own copy, not the cached object
        self.assertIsNot(first, second)
        self.assertIsNot(first, self.radar_data._combined_cache["test_variable_data"])

    def test_cached_data_not_affected_by_caller_mutation(self):
        first = self.radar_data.get_combined_data_by_variable("test_variable_data")
        expected = first.copy()
        first["new_column"] = 1
        first.drop(first.index, inplace=True)
        second = self.radar_data.get_combined_data_by_variable("test_variable_data")
        assert_frame_equal(second, expected)

    def test_clear_cache(self):
        self.radar_data.get_combined_data_by_variable("test_variable_data")
        self.radar_data.clear_cache()
        self.assertEqual(self.radar_data._combined_cache, {})
        self.radar_data.get_combined_data_by_variable("test_variable_data")
        self.radar_data.set_data({"test_user_data": self.radar_user_data})
        self.assertEqual(self.radar_data._combined_cache, {})

    def test_spark_combined_data_is_cached(self):
        radar_data = RadarData({"test_user_data": self.radar_user_data},
                               df_type="spark")
        first = radar_data.get_combined_data_by_variable("test_variable_data")
        second = radar_data.get_combined_data_by_variable("test_variable_data")
        # Spark DataFrames are immutable, so the same object can be shared
        self.assertIs(first, second)
        radar_data.clear_cache()
        self.assertEqual(radar_data._combined_cache, {})
