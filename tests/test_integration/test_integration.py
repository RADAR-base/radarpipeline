import unittest
from radarpipeline.project import Project
import yaml
from radarpipeline.radarpipeline import radarpipeline
from radarpipeline.project.validations import ConfigValidator
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from time import sleep


class TestIntegration(unittest.TestCase):

    def setUp(self):
        self.output_dir = None

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
        self.output_dir = project.config['output']["config"]['target_path']
        path = pl.Path(os.path.join(self.output_dir,
                                    "phone_battery_charging_duration.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_output_path = "tests/resources/expected_output"
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "phone_battery_charging_duration.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True), check_datetimelike_compat=True)
        path.unlink()
        path = pl.Path(os.path.join(self.output_dir, "step_count_per_day.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "step_count_per_day.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True))
        path.unlink()

    def test_multiple_feature_run(self):
        try:
            radarpipeline.run(
                "tests/resources/test_yamls/config_with_multiple_features.yaml")
            raised = False
        except Exception:
            raised = True
        self.assertFalse(raised, 'Exception raised')
        project = Project(
            input_data="tests/resources/test_yamls/config_with_multiple_features.yaml")
        # Assert if output files are created
        self.output_dir = project.config['output']["config"]['target_path']
        path = pl.Path(os.path.join(self.output_dir,
                                    "phone_battery_charging_duration.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_output_path = "tests/resources/expected_output"
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "phone_battery_charging_duration.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True), check_datetimelike_compat=True)
        path.unlink()

        path = pl.Path(os.path.join(self.output_dir, "step_count_per_day.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_df = pd.read_csv(os.path.join(expected_output_path,
                                               "step_count_per_day.csv"))
        actual_df = pd.read_csv(path)
        assert_frame_equal(expected_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'date'])
                           .reset_index(drop=True))
        path.unlink()

        path = pl.Path(os.path.join(
            self.output_dir,
            "tabularize_features_android_phone_battery_level.csv"))
        self.assertIsFile(path)
        # read the file and verify that the output is the same
        expected_output_path = "tests/resources/expected_output/tabular/"
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

    def rmtree(self, f: pl.Path):
        if f.is_file():
            f.unlink()
        else:
            for child in f.iterdir():
                self.rmtree(child)
            f.rmdir()

    def test_read(self):
        # Test the read function
        try:
            data = radarpipeline.read("tests/resources/test_data",
                                      ["android_phone_step_count"])
            MOCK_PATH = ("tests/resources/test_data/test_participant/"
                         "android_phone_step_count/0000_11.csv.gz")
            self.mock_df = pd.read_csv(MOCK_PATH)
            self.mock_df['value.time'] = pd.to_datetime(self.mock_df['value.time'],
                                                        unit='s').astype(
                                                            "datetime64[us]")
            self.mock_df['value.timeReceived'] = pd.to_datetime(
                self.mock_df['value.timeReceived'], unit='s').astype("datetime64[us]")
            self.assertIsInstance(data.get_combined_data_by_variable(
                "android_phone_step_count"), pd.DataFrame)
            assert_frame_equal(data.get_combined_data_by_variable(
                "android_phone_step_count"), self.mock_df, check_dtype=False)
        except Exception as e:
            self.fail(f"radarpipeline.read raised an exception: {e}")

    def test_read_single_variable(self):
        # Test the read function
        try:
            data = radarpipeline.read("tests/resources/test_data",
                                      "android_phone_step_count")
            MOCK_PATH = ("tests/resources/test_data/test_participant/"
                         "android_phone_step_count/0000_11.csv.gz")
            self.mock_df = pd.read_csv(MOCK_PATH)
            self.mock_df['value.time'] = pd.to_datetime(self.mock_df['value.time'],
                                                        unit='s').astype(
                                                            "datetime64[us]")
            self.mock_df['value.timeReceived'] = pd.to_datetime(
                self.mock_df['value.timeReceived'], unit='s').astype("datetime64[us]")
            self.assertIsInstance(data.get_combined_data_by_variable(
                "android_phone_step_count"), pd.DataFrame)
            assert_frame_equal(data.get_combined_data_by_variable(
                "android_phone_step_count"), self.mock_df, check_dtype=False)
        except Exception as e:
            self.fail(f"radarpipeline.read raised an exception: {e}")

#    def test_fetch(self):
#        # Test the fetch function
#        try:
#            data = radarpipeline.fetch("tests/resources/sample_data_source")
#            self.assertIsInstance(data, pd.DataFrame)
#        except Exception as e:
#            self.fail(f"radarpipeline.fetch raised an exception: {e}")
#
    def test_generate_config(self):
        # Test the generate_config function
        try:
            radarpipeline.generate_config(
                "tests/resources/sample_config.yaml")
            config = yaml.load(
                open("tests/resources/sample_config.yaml"),
                Loader=yaml.FullLoader)
            self.assertIsInstance(config, dict)
            valid_input_formats = ["csv", "csv.gz"]
            valid_output_formats = ["csv"]
            validator = ConfigValidator(config, valid_input_formats,
                                        valid_output_formats)
            validator.validate()
            # delete the sample config file
            os.remove("tests/resources/sample_config.yaml")
        except YAMLValidationError as e:
            self.fail(
                f"radarpipeline.generate_config raised a YAMLValidationError: {e}")
        except Exception as e:
            self.fail(f"radarpipeline.generate_config raised an exception: {e}")

    def test_convert_yaml(self):
        # Test the convert function
        try:
            radarpipeline.convert(
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

    def test_convert(self):
        # Test the convert function
        try:
            radarpipeline.convert(source_path="mockdata/mockdata",
                                  destination_path="output/tabular/",
                                  variables=["android_phone_step_count",
                                             "android_phone_battery_level"])
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

    def test_compute_features(self):
        # Test the compute_features function
        input_config={
            "source_type": "local",
            "config": {
                "source_path": "mockdata/mockdata"
            },
            "data_format": "csv"
        }
        feature_config={
            "location": "custom",
            "feature_groups": ["Tabularize"],
            "feature_names": [["android_phone_battery_level"]]
        }
        output = radarpipeline.compute_features(input_config, feature_config)
        actual_df = output['TabularizeFeatures']['android_phone_battery_level']
        expected_output_path = "tests/resources/expected_output/tabular"
        expected_df = pd.read_csv(
            os.path.join(expected_output_path,
                         "tabularize_features_android_phone_battery_level.csv"))
        expected_df['value.time'] = pd.to_datetime(expected_df['value.time']).astype("datetime64[us]")
        expected_df['value.timeReceived'] = pd.to_datetime(
            expected_df['value.timeReceived']).astype("datetime64[us]")
        assert_frame_equal(expected_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True),
                           actual_df.sort_values(['key.userId', 'value.time'])
                           .reset_index(drop=True), check_datetimelike_compat=True)

    def tearDown(self):
        # Delete the output directory after the test
        # check if self.output_dir
        if self.output_dir:
            self.rmtree(pl.Path(self.output_dir))