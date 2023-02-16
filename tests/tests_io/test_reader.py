import unittest
from radarpipeline.io import SparkCSVDataReader, AvroSchemaReader
import pandas as pd
from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
from pandas.testing import assert_frame_equal
from numpy.testing import assert_array_equal

class TestSparkCSVDataReader(unittest.TestCase):
    def setUp(self):
        mock_config = {"local_directory": "tests/resources/test_data/"}
        data_list = ['android_phone_step_count']
        self.sparkcsvdatareader = SparkCSVDataReader(mock_config, required_data=data_list)
        PANDAS_MOCK_PATH = "tests/resources/test_data/test_participant/android_phone_step_count/test_variable_data.csv.gz"
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.radar_variable_data = RadarVariableData(self.mock_pandas)
        self.radar_user_data = RadarUserData({"android_phone_step_count": self.radar_variable_data})
        self.radar_data = RadarData({"test_participant": self.radar_user_data})

    def test_read_data(self):
        spark_data = self.sparkcsvdatareader.read_data()
        assert_array_equal(spark_data.get_data()["test_participant"].get_data()["android_phone_step_count"].get_data().values, self.mock_pandas.values)
        self.assertTrue(isinstance(spark_data, RadarData))
        self.assertTrue(isinstance(spark_data.get_data()["test_participant"], RadarUserData))
        self.assertTrue(isinstance(spark_data.get_data()["test_participant"].get_data()["android_phone_step_count"], RadarVariableData))

