import unittest
from radarpipeline.io import SparkCSVDataReader, AvroSchemaReader
import pandas as pd
import radarpipeline
from radarpipeline.datalib import RadarVariableData, RadarUserData, RadarData
from pandas.testing import assert_frame_equal
from numpy.testing import assert_array_equal


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
