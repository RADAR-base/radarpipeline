import pyspark.sql as ps
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType
from pyspark.sql.utils import IllegalArgumentException
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class SparkEngine():
    """
    Read CSV data from local directory using pySpark
    """

    def __init__(self, spark_config: Dict = None):
        default_spark_config = {'spark_master': 'local',
                                'spark.executor.instances': 2,
                                'spark.driver.memory': '10G',
                                'spark.executor.cores': 4,
                                'spark.executor.memory': '10g',
                                'spark.memory.offHeap.enabled': True,
                                'spark.memory.offHeap.size': '20g',
                                'spark.driver.maxResultSize': '0',
                                'spark.log.level': "OFF"}
        self.spark_config = default_spark_config
        if spark_config is not None:
            self.spark_config.update(spark_config)

    def initialize_spark_session(self) -> ps.SparkSession:
        """
        Initializes and returns a SparkSession

        Returns
        -------
        SparkSession
            A SparkSession object
        """

        """
        Spark configuration documentation:
        https://spark.apache.org/docs/latest/configuration.html

        `spark.executor.instances` is the number of executors to
        launch for an application.

        `spark.executor.cores` is the number of cores to =
        use on each executor.

        `spark.executor.memory` is the amount of memory to
        use per executor process.

        `spark.driver.memory` is the amount of memory to use for the driver process,
        i.e. where SparkContext is initialized, in MiB unless otherwise specified.

        `spark.memory.offHeap.enabled` is to enable off-heap memory allocation

        `spark.memory.offHeap.size` is the absolute amount of memory which can be used
        for off-heap allocation, in bytes unless otherwise specified.

        `spark.driver.maxResultSize` is the limit of total size of serialized results of
        all partitions for each Spark action (e.g. collect) in bytes.
        Should be at least 1M, or 0 for unlimited.
        """
        if self.spark_config['spark_master'] == "local":
            self.spark = (
                SparkSession.builder.master("local[*]").appName("radarpipeline")
                .config('spark.executor.instances',
                        self.spark_config['spark.executor.instances'])
                .config('spark.executor.cores',
                        self.spark_config['spark.executor.cores'])
                .config('spark.executor.memory',
                        self.spark_config['spark.executor.memory'])
                .config('spark.driver.memory',
                        self.spark_config['spark.driver.memory'])
                .config('spark.memory.offHeap.enabled',
                        self.spark_config['spark.memory.offHeap.enabled'])
                .config('spark.memory.offHeap.size',
                        self.spark_config['spark.memory.offHeap.size'])
                .config('spark.driver.maxResultSize',
                        self.spark_config['spark.driver.maxResultSize'])
                .config('spark.log.level',
                        self.spark_config['spark.log.level'])
                .getOrCreate()
            )
        else:
            self.spark = (
                SparkSession.builder.master(
                    self.spark_config['spark_master']).appName("radarpipeline")
                .getOrCreate()
            )
        self.spark._jsc.setLogLevel(self.spark_config['spark.log.level'])
        self.spark.sparkContext.setLogLevel("OFF")
        # Enable Apache Arrow for optimizations in Spark to Pandas conversion
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Fallback to use non-Arrow conversion in case of errors
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled",
                            "true")
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        # For further reading:
        # https://spark.apache.org/docs/3.0.1/sql-pyspark-pandas-with-arrow.html
        logger.info("Spark Session created")
        return self.spark

    def close_spark_session(self):
        self.spark.stop()
