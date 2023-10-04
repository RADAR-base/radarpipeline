import unittest
from radarpipeline.io import SparkDataWriter, PandasDataWriter
from radarpipeline.common import constants
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
        PANDAS_MOCK_PATH = ("tests/resources/test_data/test_participant/"
                            "android_phone_step_count/0000_11.csv.gz")
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.mock_pandas['key.projectId'] = self.mock_pandas['key.projectId'] \
            .str \
            .strip()
        spark = SparkSession.builder \
            .master("local") \
            .appName("radarpipeline") \
            .getOrCreate()
        # Create PySpark DataFrame from Pandas
        self.sparkDF = spark.createDataFrame(self.mock_pandas)

        self.features = {"test_feature": self.sparkDF}

    def test_write_data_csv(self):
        self.sparkdatawriter = SparkDataWriter(self.features, self.output_dir)
        self.sparkdatawriter.write_data()
        # check if the dir exists
        self.assertTrue(os.path.exists(f"{self.output_dir}" + "test_feature"))
        # check if the file has the correct content
        ## spark read the directory
        spark = SparkSession.builder \
            .master("local") \
            .appName("radarpipeline") \
            .getOrCreate()
        output_file = spark.read.csv(f"{self.output_dir}" + "test_feature",
                                     header=True,
                                     schema=self.sparkDF.schema,
                                     sep=constants.CSV_DELIMITER,
                                     encoding=constants.ENCODING,
                                     lineSep=constants.LINESEP)
        assert_pyspark_df_equal(output_file, self.sparkDF)
        assert_frame_equal(output_file.toPandas(), self.mock_pandas)

    def test_write_data_parquet(self):
        self.sparkdatawriter = SparkDataWriter(self.features, self.output_dir,
                                               data_format="parquet")
        self.sparkdatawriter.write_data()
        # check if the dir exists
        self.assertTrue(os.path.exists(f"{self.output_dir}" + "test_feature"))
        # check if the file has the correct content
        ## spark read the directory
        spark = SparkSession.builder \
            .master("local") \
            .appName("radarpipeline") \
            .getOrCreate()
        output_file = spark.read.parquet(f"{self.output_dir}" + "test_feature")
        assert_pyspark_df_equal(output_file, self.sparkDF)
        assert_frame_equal(output_file.toPandas(), self.mock_pandas)

    def tearDown(self):
        # delete the file
        path = pl.Path(f"{self.output_dir}" + "test_feature")
        shutil.rmtree(path)


class TestPandasDataWriter(unittest.TestCase):
    def setUp(self):
        # TODO: Make self.output_dir a temporary directory
        self.output_dir = "tests/resources/test_output/"
        PANDAS_MOCK_PATH = ("tests/resources/test_data/test_participant/"
                            "android_phone_step_count/0000_11.csv.gz")
        self.mock_pandas = pd.read_csv(PANDAS_MOCK_PATH)
        self.features = {"test_feature": self.mock_pandas}

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def assertIsDirectory(self, path):
        if not pl.Path(path).resolve().is_dir():
            raise AssertionError("Directory does not exist: %s" % str(path))

    def test_write_data_csv(self):
        pandasdatawriter = PandasDataWriter(self.features, self.output_dir)
        pandasdatawriter.write_data()
        # check if the file exists
        self.path = pl.Path(f"{self.output_dir}" + "test_feature.csv")
        self.assertIsFile(self.path)
        # check if the file has the correct content
        output_file = pd.read_csv(self.path)
        assert_frame_equal(output_file, self.mock_pandas)

    def test_write_data_parquet(self):
        pandasdatawriter = PandasDataWriter(self.features, self.output_dir,
                                            data_format="parquet")
        pandasdatawriter.write_data()
        # check if the file exists
        self.path = pl.Path(f"{self.output_dir}" + "test_feature.parquet")
        self.assertIsFile(self.path)
        # check if the file has the correct content
        output_file = pd.read_parquet(self.path)
        assert_frame_equal(output_file, self.mock_pandas)

    def test_write_data_pickle(self):
        pandasdatawriter = PandasDataWriter(self.features, self.output_dir,
                                            data_format="pickle")
        pandasdatawriter.write_data()
        # check if the file exists
        self.path = pl.Path(f"{self.output_dir}" + "test_feature.pkl")
        self.assertIsFile(self.path)
        # check if the file has the correct content
        output_file = pd.read_pickle(self.path)
        assert_frame_equal(output_file, self.mock_pandas)

    def tearDown(self):
        # delete the file
        self.path.unlink()
        del self.path
