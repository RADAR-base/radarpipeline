import unittest
from radarpipeline.project.validations import ConfigValidator
from copy import deepcopy


MOCK_CONFIG = {
    'project': {
        'project_name' : 'mock_project',
        'description': 'mock_description',
        'version': 'mock_version'},
    'input': {
        'data_type': 'mock',
        'config': {
            'source_path': 'mockdata/mockdata'},
        'data_format': 'csv'},
    'configurations': {'df_type': 'pandas'},
    'features': [
        {'location':
            'https://github.com/RADAR-base-Analytics/mockfeatures',
            'branch': 'main',
            'feature_groups': ['MockFeatureGroup'],
            'feature_names': [['all']]}],
    'output': {
        'output_location': 'local',
        'config': {'target_path': 'output/mockdata'},
        'data_format': 'csv',
        'compress': False}}


class TestConfigValidatorInput(unittest.TestCase):
    def setUp(self):
        self.valid_input_formats = ["csv", "csv.gz"]
        self.valid_output_formats = ["csv"]
        self.mock_config = deepcopy(MOCK_CONFIG)

    def test_validate_mock(self):
        validator = ConfigValidator(self.mock_config, self.valid_input_formats,
                                    self.valid_output_formats)
        validator.validate()

    def test_validate_sftp(self):
        config = self.mock_config
        config['input']['data_type'] = 'sftp'
        config['input']['config'] = {
            "sftp_host": "mock_host",
            "sftp_source_path": "mock_source_path",
            "sftp_username": "mock_username",
            "sftp_private_key": "mock_private_key",
            "sftp_target_path": "mock_target_path"
        }
        validator = ConfigValidator(config, self.valid_input_formats,
                                    self.valid_output_formats)
        validator.validate()

    def test_validate_invalid_sftp(self):
        config = self.mock_config
        config['input']['data_type'] = 'sftp'
        config['input']['config'] = {
            "sftp_host": "mock_host",
            "sftp_source_path": "mock_source_path",
            "sftp_username": "mock_username",
            "sftp_target_path": "mock_target_path"
        }
        validator = ConfigValidator(config, self.valid_input_formats,
                                    self.valid_output_formats)
        with self.assertRaises(ValueError):
            validator.validate()

    def test_validate_local(self):
        config = self.mock_config
        config['input']['data_type'] = 'local'
        config['input']['config'] = {
            "source_path": "mockdata/mockdata"
        }
        validator = ConfigValidator(config, self.valid_input_formats,
                                    self.valid_output_formats)
        validator.validate()

    def test_validate_local_wrong_source_path(self):
        config = self.mock_config
        config['input']['data_type'] = 'local'
        config['input']['config'] = {
            "source_path": "xyz"
        }
        validator = ConfigValidator(config, self.valid_input_formats,
                                    self.valid_output_formats)
        with self.assertRaises(ValueError):
            validator.validate()

    def test_validate_invalid_input_format(self):
        config = self.mock_config
        config.pop('input')
        validator = ConfigValidator(config, self.valid_input_formats,
                                    self.valid_output_formats)
        with self.assertRaises(ValueError):
            validator.validate()


class TestConfigValidatorConfigurations(unittest.TestCase):
    def setUp(self):
        self.valid_input_formats = ["csv", "csv.gz"]
        self.valid_output_formats = ["csv"]
        self.mock_config = deepcopy(MOCK_CONFIG)

    def test_validate_configurations(self):
        self.mock_config['configurations']['df_type'] = 'invalid'
        validator = ConfigValidator(self.mock_config, self.valid_input_formats,
                                    self.valid_output_formats)
        with self.assertRaises(ValueError):
            validator.validate()


class TestConfigValidatorFeatures(unittest.TestCase):
    def setUp(self):
        self.valid_input_formats = ["csv", "csv.gz"]
        self.valid_output_formats = ["csv"]
        self.mock_config = deepcopy(MOCK_CONFIG)

    def test_is_feature_empty(self):
        self.mock_config['features'] = []
        validator = ConfigValidator(self.mock_config, self.valid_input_formats,
                                    self.valid_output_formats)
        with self.assertRaises(ValueError):
            validator.validate()
