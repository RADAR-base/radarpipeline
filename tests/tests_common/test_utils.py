from radarpipeline.common.utils import is_valid_github_path, read_yaml
import unittest
import os


def test_is_valid_github_path():
    assert is_valid_github_path("https://github.com/RADAR-base/radarpipeline") is True


class TestReadYaml(unittest.TestCase):

    def setUp(self):
        self.TESTDATA_FILENAME = "config.yaml"
        self.TESTDATA_FILENAME_WRONG = "tests/resources/config.yaml"
        self.TESTDATA_FILENAME_EMPTY = "tests/resources/test_config.yaml"

    def test_read_correct_yaml(self):
        config = read_yaml(self.TESTDATA_FILENAME)
        expected_config = {
            'project': {
                'project_name': 'mock_project',
                'description': 'mock_description',
                'version': 'mock_version'},
            'input_data': {
                'data_location': 'local',
                'local_directory': 'mockdata/mockdata',
                'data_format': 'csv'},
            'configurations': {'df_type': 'pandas'},
            'features': [{
                'location': 'https://github.com/RADAR-base-Analytics/mockfeatures',
                'branch': 'main',
                'feature_groups': ['MockFeatureGroup'],
                'feature_names': [['all']]}],
            'output_data': {
                'output_location': 'local',
                'local_directory': 'output/mockdata',
                'data_format': 'csv',
                'compress': False}}
        self.assertDictEqual(config, expected_config)

    def test_read_unavailable_yaml(self):
        with self.assertRaises(ValueError):
            read_yaml(self.TESTDATA_FILENAME_WRONG)
            read_yaml(self.TESTDATA_FILENAME_EMPTY)
