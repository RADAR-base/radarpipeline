from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
import unittest
import os
import pandas as pd
from pandas.testing import assert_frame_equal


class TestRadarData(unittest.TestCase):
    def setUp(self):
        PANDAS_MOCK_PATH = ("tests/resources/test_data/test_participant/"
                            "android_phone_step_count/test_variable_data.csv.gz")
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.radar_variable_data = RadarVariableData(self.mock_pandas)
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
        assert_frame_equal(self.radar_data.get_combined_data_by_variable(
            "test_variable_data"),
            self.mock_pandas)
