import unittest
from radarpipeline.io import SparkDataWriter, PandasDataWriter
import pandas as pd
import pathlib as pl
import os
import shutil
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from numpy.testing import assert_array_equal
from pyspark_test import assert_pyspark_df_equal



class TestSparkDataWriter(unittest.TestCase):
    def setUp(self):
        self.output_dir = "tests/resources/test_output/"
        PANDAS_MOCK_PATH = "tests/resources/test_data/test_participant/android_phone_step_count/test_variable_data.csv.gz"
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        spark = SparkSession.builder.master("local").appName("radarpipeline").getOrCreate()
        #Create PySpark DataFrame from Pandas
        self.sparkDF = spark.createDataFrame(self.mock_pandas )

        self.features = {"test_feature": self.sparkDF }
        self.sparkdatawriter = SparkDataWriter(self.features, self.output_dir)

    def test_write_data(self):
        self.sparkdatawriter.write_data()
        # check if the dir exists
        self.assertTrue(os.path.exists(f"{self.output_dir}" + "test_feature"))
        # check if the file has the correct content
        ## spark read the directory
        spark = SparkSession.builder.master("local").appName("radarpipeline").getOrCreate()
        output_file = spark.read.csv(f"{self.output_dir}" + "test_feature",  header=True, schema=self.sparkDF.schema)
        assert_pyspark_df_equal(output_file, self.sparkDF)

    def tearDown(self):
        # delete the file
        path = pl.Path(f"{self.output_dir}" + "test_feature")
        shutil.rmtree(path)

class TestPandasDataWriter(unittest.TestCase):
    def setUp(self):
        # TODO: Make self.output_dir a temporary directory
        self.output_dir = "tests/resources/test_output/"
        PANDAS_MOCK_PATH = "tests/resources/test_data/test_participant/android_phone_step_count/test_variable_data.csv.gz"
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.features = {"test_feature": self.mock_pandas }
        self.sparkdatawriter = PandasDataWriter(self.features, self.output_dir)

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def test_write_data(self):
        self.sparkdatawriter.write_data()
        # check if the file exists
        path = pl.Path(f"{self.output_dir}" + "test_feature.csv")
        self.assertIsFile(path)
        # check if the file has the correct content
        output_file = pd.read_csv(path)
        assert_frame_equal(output_file, self.mock_pandas)

    def tearDown(self):
        # delete the file
        path = pl.Path(f"{self.output_dir}" + "test_feature.csv")
        path.unlink()