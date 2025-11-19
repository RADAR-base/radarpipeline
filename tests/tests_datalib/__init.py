from radarpipeline.project.sparkengine import SparkEngine
import unittest
import pyspark.sql.functions as f
from pyspark.sql.types import TimestampType
from radarpipeline.common.utils import preprocess_time_data


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark_engine = SparkEngine()
        cls.spark = cls.spark_engine.initialize_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark_engine.close_spark_session()

    def preprocess_data(self, data):
        return preprocess_time_data(data)