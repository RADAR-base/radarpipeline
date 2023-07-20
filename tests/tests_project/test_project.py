import unittest
from radarpipeline.project import Project
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os
from strictyaml import as_document
from radarpipeline.common import utils

import requests
import yaml


class TestProjectInit(unittest.TestCase):
    def test_init_config(self):
        config_path = 'tests/resources/test_yamls/test_config_project.yaml'
        try:
            Project(config_path)
            raised = False
        except Exception:
            raised = True
        self.assertFalse(raised, 'Exception raised')

    def test_init_config_incomplete(self):
        config_path = 'tests/resources/test_yamls/test_config_incomplete.yaml'
        with self.assertRaises(SystemExit):
            Project(config_path)

    def test_init_input_config_invalid(self):
        config_path = 'tests/resources/test_yamls/test_config_invalid.yaml'
        with self.assertRaises(Exception):
            Project(config_path)


class TestProject(unittest.TestCase):
    '''
    Test running mock project
    '''
    def setUp(self):
        config_path = "tests/resources/test_yamls/test_config.yaml"
        self.project = Project(config_path)

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def test_fetch_data(self):
        self.project.fetch_data()
        self.assertTrue(self.project.data is not None, 'Data not fetched')

    def test_compute_features(self):
        self.project.fetch_data()
        self.project.compute_features()
        self.assertTrue(self.project.features is not None, 'Features not computed')

    def test_export_data(self):
        self.project.fetch_data()
        self.project.compute_features()
        self.project.export_data()
        self.output_dir = self.project.config['output']['config']['target_path']
        path = pl.Path(os.path.join(self.output_dir,
                                    "phone_battery_charging_duration.csv"))
        self.assertIsFile(path)
        path.unlink()
        path = pl.Path(os.path.join(self.output_dir, "step_count_per_day.csv"))
        self.assertIsFile(path)
        path.unlink()

    def test_get_total_required_data(self):
        required_data_output = self.project._get_total_required_data()
        expected_data = ['android_phone_battery_level', 'android_phone_step_count']
        self.assertListEqual(sorted(required_data_output), sorted(expected_data))

    def tearDown(self) -> None:
        del self.project


class TestProjectRemoteLink(unittest.TestCase):
    def setUp(self):
        self.remotelink = "https://github.com/RADAR-base-Analytics/mockfeatures"
        self.remotelink_raw = "https://raw.githubusercontent.com/"\
            "RADAR-base-Analytics/mockfeatures/main/config.yaml"
        response = requests.get(self.remotelink_raw , allow_redirects=True)
        content = response.content.decode("utf-8")
        self.expected_config = yaml.safe_load(content)

    def test_resolve_remote_link(self):
        project = Project(self.remotelink)
        project_config_file = project._resolve_input_data(self.remotelink)
        project_config = yaml.safe_load(open(project_config_file, 'r'))
        # Assert that config file is as same as it is in the remote link
        self.assertDictEqual(project_config, self.expected_config)

    def test_get_config(self):
        project = Project(self.remotelink)
        project_config = project._get_config()
        schema = utils.get_yaml_schema()
        print(project_config)
        print(self.expected_config)
        expected_config_updated = as_document(self.expected_config, schema).data
        self.assertDictEqual(project_config, expected_config_updated)
