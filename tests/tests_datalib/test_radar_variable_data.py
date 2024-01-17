from radarpipeline.datalib import RadarVariableData
from radarpipeline.common.utils import PySparkTestCase
import unittest
import os
import pandas as pd
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as f
from pyspark.testing import assertDataFrameEqual


class TestRadarVariableData(PySparkTestCase):

    def setUp(self):
        MOCK_PATH = ("tests/resources/test_data/test_participant/"
                     "android_phone_step_count/0000_11.csv.gz")
        self.mock_df = self.spark.read.csv(MOCK_PATH,
                                           header=True,
                                           inferSchema=True)
        self.mock_df = self.preprocess_data(self.mock_df)
        self.radar_variable_data = RadarVariableData(self.mock_df)

    def test_get_data(self):
        assertDataFrameEqual(self.radar_variable_data.get_data(), self.mock_df,
                             checkRowOrder=True)

    def test_get_data_keys(self):
        self.assertEqual(self.radar_variable_data.get_data_keys(),
                         list(self.mock_df.columns))

    def test_get_data_sizes(self):
        self.assertEqual(self.radar_variable_data.get_data_size(),
                         int(self.mock_df.count()))

