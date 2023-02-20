import unittest
from radarpipeline import radarpipeline, Project
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os
import pandas as pd
from pandas.testing import assert_frame_equal


class TestIntegration(unittest.TestCase):

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def test_run(self):
        # Test that the pipeline runs without error
        # This is a very basic test, but it's a start
        # Assert if pipeline is throwring an error
        try:
            radarpipeline.run()
            raised = False
        except Exception:
            raised = True
        self.assertFalse(raised, 'Exception raised')
        project = Project(input_data="config.yaml")
        # Assert if output files are created
        self.output_dir = project.config['output_data']['local_directory']
        path = pl.Path(os.path.join(self.output_dir,
                                    "phone_battery_charging_duration.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_output_path = "tests/resources/expected_output"
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "phone_battery_charging_duration.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df, actual_df)
        path.unlink()
        path = pl.Path(os.path.join(self.output_dir, "step_count_per_day.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "step_count_per_day.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df, actual_df)
        path.unlink()
