import logging

from radarpipeline.io.reader import Reader
from radarpipeline.project.sparkengine import SparkEngine

from typing import Dict

logger = logging.getLogger(__name__)


class CustomDataReader():
    def __init__(self, input_config, variables, data_type="local", data_format="csv",
                 df_type="pandas") -> None:
        self.variables = variables
        self.data_format = data_format
        self.data_type = data_type
        self.config = self.modify_config(input_config, data_format)
        self.sparkengine = SparkEngine()
        self.spark = self.sparkengine.initialize_spark_session()
        self.data_reader = Reader(self.spark, self.config, variables, df_type)

    def modify_config(self, input_config, data_format) -> Dict:
        """
        Modify the input configuration to include the variables of interest
        """
        config = {'input': {}}
        config['input'] = input_config
        config['input']['data_format'] = data_format
        config['input']['data_type'] = self.data_type
        return config

    def read_data(self):
        return self.data_reader.read_data()

    def close_session(self):
        self.sparkengine.close_spark_session()
