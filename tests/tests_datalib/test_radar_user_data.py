from radarpipeline.datalib import RadarVariableData, RadarUserData
from radarpipeline.common.utils import PySparkTestCase
import unittest
import os
import pandas as pd
from pyspark.testing import assertDataFrameEqual


class TestRadarUserData(PySparkTestCase):
    def setUp(self):
        MOCK_PATH = ("tests/resources/test_data/test_participant/"
                     "android_phone_step_count/0000_11.csv.gz")
        self.mock_df = self.spark.read.csv(MOCK_PATH,
                                           header=True,
                                           inferSchema=True)
        self.radar_variable_data = RadarVariableData(self.mock_df)
        self.radar_user_data = RadarUserData({"test_variable_data":
                                              self.radar_variable_data})

    def test_get_data(self):
        self.assertDictEqual(self.radar_user_data.get_data(),
                             {"test_variable_data": self.radar_variable_data})

    def test_get_data_keys(self):
        self.assertEqual(self.radar_user_data.get_data_keys(), ["test_variable_data"])

    def test_get_data_sizes(self):
        self.assertEqual(self.radar_user_data.get_data_size(), 1)

    def test_get_data_by_variable(self):
        self.assertEqual(self.radar_user_data.get_data_by_variable(
            "test_variable_data"),
            self.radar_variable_data)
