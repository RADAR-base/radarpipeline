import unittest
from radarpipeline.project import SparkEngine


class TestSparkDefaultConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.default_spark_config = {'spark.executor.instances': 6,
                                    'spark.driver.memory': '10G',
                                    'spark.executor.cores': 4,
                                    'spark.executor.memory': '10g',
                                    'spark.memory.offHeap.enabled': "true",
                                    'spark.memory.offHeap.size': '20g',
                                    'spark.driver.maxResultSize': '0',
                                    'spark.log.level': "OFF"}
        cls.spark_engine = SparkEngine()
        cls.spark = cls.spark_engine.initialize_spark_session()

    def test_spark_config(self):
        spark_config_output = dict(self.spark.sparkContext.
                                   getConf().getAll())
        for key, value in self.default_spark_config.items():
            self.assertEqual(spark_config_output[key], str(value))

    def test_spark_config_dict(self):
        spark_config_output = self.default_spark_config
        self.assertDictEqual(self.default_spark_config, spark_config_output)

    @classmethod
    def tearDownClass(cls):
        cls.spark_engine.close_spark_session()


class TestSparkCustomConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark_config = {
            "spark.executor.instances": "3",
            "spark.memory.offHeap.enabled": "false",
            "spark.executor.cores": 2,
            "spark.executor.memory": "5g",
            "spark.driver.memory": "10g",
            "spark.memory.offHeap.size": "10g",
            "spark.driver.maxResultSize": "0",
            "spark.log.level": "ERROR"}
        cls.spark_engine = SparkEngine(spark_config=cls.spark_config)
        cls.spark = cls.spark_engine.initialize_spark_session()

    def test_spark_config(self):
        spark_config_output = dict(self.spark.sparkContext.
                                   getConf().getAll())
        for key, value in self.spark_config.items():
            self.assertEqual(spark_config_output[key], str(value))

    def test_spark_config_dict(self):
        spark_config_output = self.spark_config
        self.assertDictEqual(self.spark_config, spark_config_output)

    @classmethod
    def tearDownClass(cls):
        cls.spark_engine.close_spark_session()
