from radarpipeline.datalib import RadarVariableData, RadarUserData
import unittest
import  os
import pandas as pd
from pandas.testing import assert_frame_equal

class TestRadarUserData(unittest.TestCase):
    def setUp(self):
        PANDAS_MOCK_PATH = "tests/resources/test_participant/android_phone_step_count/test_variable_data.csv.gz"
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.radar_variable_data = RadarVariableData(self.mock_pandas)
        self.radar_user_data = RadarUserData({"test_variable_data": self.radar_variable_data})

    def test_get_data(self):
        self.assertDictEqual(self.radar_user_data.get_data(), {"test_variable_data": self.radar_variable_data})

    def test_get_data_keys(self):
        self.assertEqual(self.radar_user_data.get_data_keys(), ["test_variable_data"])

    def test_get_data_sizes(self):
        self.assertEqual(self.radar_user_data.get_data_size(), 1)

    def test_get_data_by_variable(self):
        self.assertEqual(self.radar_user_data.get_data_by_variable("test_variable_data"), self.radar_variable_data)