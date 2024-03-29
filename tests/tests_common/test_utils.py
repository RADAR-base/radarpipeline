import pytest
from pyspark.sql import Row, SparkSession
import unittest
import os
from radarpipeline.common.utils import is_valid_github_path, read_yaml
from radarpipeline.common import utils
from strictyaml.exceptions import YAMLValidationError

RADAR_PIPELINE_URL = "https://github.com/RADAR-base/radarpipeline"
WRONG_GITHUB_URL = "https://githuib.com/RADAR-base/radarpipeline"
NONEXISTENT_REPO_URL = "https://github.com/RADAR-base/notradarpipeline"
RADARPIPELINE_NAME = "radarpipeline"


def init_spark():
    spark = (
        SparkSession.builder.master("local")
        .appName("radar-common-utils-test")
        .getOrCreate()
    )
    return spark


@pytest.mark.parametrize(
    "test_input, expected_output",
    [
        (RADAR_PIPELINE_URL, True),
        (WRONG_GITHUB_URL, False),
        (NONEXISTENT_REPO_URL, False),
    ],
)
def test_is_valid_github_path(test_input, expected_output):
    assert utils.is_valid_github_path(test_input) == expected_output


def test_pascal_to_snake_case():
    expected = "pascal_to_snake_case"
    test_input = "PascalToSnakeCase"
    actual = utils.pascal_to_snake_case(test_input)
    assert actual == expected


@pytest.mark.parametrize(
    "test_input, expected_output",
    [
        (RADAR_PIPELINE_URL, RADARPIPELINE_NAME),
        (RADAR_PIPELINE_URL + "/", RADARPIPELINE_NAME),
        (RADAR_PIPELINE_URL + ".git", RADARPIPELINE_NAME),
    ],
)
def test_get_repo_name_from_url(test_input, expected_output):
    assert utils.get_repo_name_from_url(test_input) == expected_output, read_yaml


def test_combine_pyspark_dfs():
    spark = init_spark()
    df1 = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["a", "b", "c"])
    df2 = spark.createDataFrame([(7, 8, 9), (10, 11, 12)], ["a", "b", "c"])
    df3 = spark.createDataFrame([(13, 14, 15), (16, 17, 18)], ["a", "b", "c"])
    combined_df = utils.combine_pyspark_dfs([df1, df2, df3])
    assert combined_df.count() == 6
    assert combined_df.columns == ["a", "b", "c"]
    assert combined_df.collect() == [
        Row(a=1, b=2, c=3),
        Row(a=4, b=5, c=6),
        Row(a=7, b=8, c=9),
        Row(a=10, b=11, c=12),
        Row(a=13, b=14, c=15),
        Row(a=16, b=17, c=18),
    ]


class TestReadYaml(unittest.TestCase):

    def setUp(self):
        self.TESTDATA_FILENAME = "tests/resources/test_yamls/test_config.yaml"
        self.TESTDATA_FILENAME_SPARK = "tests/resources/test_yamls/"\
            "config_with_spark.yaml"
        self.TESTDATA_FILENAME_INCORRECT_SPARK = "tests/resources/test_yamls/"\
            "config_with_incorrect_spark.yaml"
        self.TESTDATA_FILENAME_WRONG = "tests/resources/config.yaml"
        self.TESTDATA_FILENAME_EMPTY = "tests/resources/test_config.yaml"

    def test_read_correct_yaml(self):
        config = read_yaml(self.TESTDATA_FILENAME)
        print(config)
        expected_config = {
            'project': {
                'project_name': 'mock_project',
                'description': 'mock_description',
                'version': 'mock_version'},
            'input': {
                'data_type': 'mock',
                'config': {'source_path': 'mockdata/mockdata'},
                'data_format': 'csv'
            },
            'configurations': {'df_type': 'pandas'},
            'features': [{
                'location': 'https://github.com/RADAR-base-Analytics/mockfeatures',
                'branch': 'main',
                'feature_groups': ['MockFeatureGroup'],
                'feature_names': [['all']]}],
            'output': {
                'output_location': 'local',
                'config': {'target_path': 'output/mockdata'},
                'data_format': 'csv',
                'compress': False}}
        self.assertDictEqual(config, expected_config)

    def test_read_unavailable_yaml(self):
        with self.assertRaises(ValueError):
            read_yaml(self.TESTDATA_FILENAME_WRONG)
            read_yaml(self.TESTDATA_FILENAME_EMPTY)

    def test_read_yaml_with_spark_config(self):
        config = read_yaml(self.TESTDATA_FILENAME_SPARK)
        print(config)
        expected_config = {
            'project': {
                'project_name': 'mock_project',
                'description': 'mock_description',
                'version': 'mock_version'},
            'input': {
                'data_type': 'mock',
                'config': {'source_path': 'mockdata/mockdata'},
                'data_format': 'csv'
            },
            'configurations': {'df_type': 'pandas'},
            'features': [{
                'location': 'https://github.com/RADAR-base-Analytics/mockfeatures',
                'branch': 'main',
                'feature_groups': ['MockFeatureGroup'],
                'feature_names': [['all']]}],
            'output': {
                'output_location': 'local',
                'config': {'target_path': 'output/mockdata'},
                'data_format': 'csv',
                'compress': False},
            'spark_config': {
                "spark.executor.instances": 2,
                "spark.memory.offHeap.enabled": False,
                "spark.executor.cores": 4,
                "spark.executor.memory": "10g",
                "spark.driver.memory": "15g",
                "spark.memory.offHeap.size": "20g",
                "spark.driver.maxResultSize": "0"}}
        self.assertDictEqual(config, expected_config)

    def test_read_yaml_with_incorrect_spark_config(self):
        with self.assertRaises(YAMLValidationError):
            read_yaml(self.TESTDATA_FILENAME_INCORRECT_SPARK)
