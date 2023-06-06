import unittest
from radarpipeline.project import Project
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os


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
        config_path = 'config.yaml'
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

    def tearDown(self) -> None:
        del self.project
