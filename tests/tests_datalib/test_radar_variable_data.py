from radarpipeline.datalib import RadarVariableData
import unittest
import  os
import pandas as pd
from pandas.testing import assert_frame_equal

class TestRadarVariableData(unittest.TestCase):
    def setUp(self):
        PANDAS_MOCK_PATH = "tests/resources/test_participant/android_phone_step_count/test_variable_data.csv.gz"
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.radar_variable_data = RadarVariableData(self.mock_pandas)

    def test_get_data(self):
        assert_frame_equal(self.radar_variable_data.get_data(), self.mock_pandas)

    def test_get_data_keys(self):
        self.assertEqual(self.radar_variable_data.get_data_keys(), list(self.mock_pandas.columns))

    def test_get_data_sizes(self):
        self.assertEqual(self.radar_variable_data.get_data_size(), len(self.mock_pandas.index))