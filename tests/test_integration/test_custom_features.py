import unittest
from radarpipeline.project import Project
from radarpipeline.radarpipeline import radarpipeline
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os
import pandas as pd
from pandas.testing import assert_frame_equal


class TestCustomFeatureTabularize(unittest.TestCase):

    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

    def test_run(self):
        # Test that the pipeline runs without error
        # This is a very basic test, but it's a start
        # Assert if pipeline is throwring an error
        try:
            radarpipeline.run(
                "tests/resources/test_yamls/config_with_custom_feature.yaml")
            raised = False
        except Exception:
            raised = True
        self.assertFalse(raised, 'Exception raised')
        project = Project(
            input_data="tests/resources/test_yamls/config_with_custom_feature.yaml")
        # Assert if output files are created
        self.output_dir = project.config['output']["config"]['target_path']
        path = pl.Path(os.path.join(
            self.output_dir,
            "tabularize_features_android_phone_battery_level.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_output_path = "tests/resources/expected_output/tabular"
        expected_df = pd.read_csv(
            os.path.join(expected_output_path,
                         "tabularize_features_android_phone_battery_level.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True), check_datetimelike_compat=True)
        path.unlink()
        path = pl.Path(
            os.path.join(
                self.output_dir,
                "tabularize_features_android_phone_step_count.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_df = pd.read_csv(
            os.path.join(expected_output_path,
                         "tabularize_features_android_phone_step_count.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True))
        path.unlink()
