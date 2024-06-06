from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
import unittest
import os
import pandas as pd
from radarpipeline.common.utils import PySparkTestCase
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType
from pandas.testing import assert_frame_equal


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
