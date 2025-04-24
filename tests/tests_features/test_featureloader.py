#from radarpipeline.features import FeatureLoader
#from pandas.testing import assert_frame_equal
#import os
#import pandas as pd
#import unittest

"""
class TestFeatureLoader(unittest.TestCase):
    def setUp(self) -> None:
        self.PATH = "./mockdata/mockdata"
        self.data_vars = ["android_phone_battery_level", "android_phone_step_count"]
        self.expected_output_path = "tests/resources/expected_output"

    def test_modify_config(self):
        config = {
            "input": {"config": {"source_path": self.PATH}},
            "features": [{
                "location": 'https://github.com/RADAR-base-Analytics/mockfeatures',
                "feature_groups": ["MockFeatureGroup"],
                "feature_names": [["all"]]}]
        }
        feature_loader = FeatureLoader(config)
        self.assertEqual(feature_loader.config["project"]["project_name"], "custom")
        self.assertEqual(feature_loader.config["input"]["source_type"], "local")
        self.assertEqual(feature_loader.config["input"]["data_format"], "csv")
        self.assertEqual(feature_loader.config["output"]["output_location"],
                         "dataframe")
        self.assertEqual(feature_loader.config["configurations"]["df_type"], "pandas")

    def test_load_features(self):
        config = {
            "input": {"config": {"source_path": self.PATH}},
            "features": [{
                "location": 'https://github.com/RADAR-base-Analytics/mockfeatures',
                "feature_groups": ["MockFeatureGroup"],
                "feature_names": [["all"]]}]
        }
        feature_loader = FeatureLoader(config)
        feature_names = ['PhoneBatteryChargingDuration', 'StepCountPerDay']
        file_names = ['phone_battery_charging_duration.csv', 'step_count_per_day.csv']
        features = feature_loader.load_features()
        for i, feature_name in enumerate(feature_names):
            self.assertIn(feature_name, features)
            actual_df = features[feature_name].reset_index(drop=True)
            expected_df = pd.read_csv(os.path.join(self.expected_output_path,
                                                   file_names[i]), parse_dates=["date"])
            actual_df['date'] = pd.to_datetime(actual_df['date'])
            expected_df['date'] = pd.to_datetime(expected_df['date'])
            if 'value.statusTime' in actual_df.columns:
                actual_df.drop(columns=['value.statusTime'], inplace=True)
                expected_df.drop(columns=['value.statusTime'], inplace=True)
            assert_frame_equal(expected_df.sort_values(['key.userId', 'date'])
                               .reset_index(drop=True),
                               actual_df.sort_values(['key.userId', 'date'])
                               .reset_index(drop=True),
                               check_datetimelike_compat=True, check_dtype=False)

    def test_load_custom_features(self):
        config = {
            "input": {"config": {"source_path": self.PATH
                                 }
                      },
            "features": [
                {
                    "location": "custom",
                    "feature_groups": ["Tabularize"],
                    "feature_names": [self.data_vars]}]}
        feature_loader = FeatureLoader(config)
        feature_name = "TabularizeFeatures"
        features = feature_loader.load_features()
        for feature_key in features[feature_name]:
            actual_df = features[feature_name][feature_key]
            expected_df = pd.read_csv(os.path.join(
                self.expected_output_path,
                "tabular",
                f"tabularize_features_{feature_key}.csv"),
                parse_dates=['value.time', 'value.timeReceived'])
            actual_df['value.time'] = actual_df['value.time'].astype('datetime64[ns]')
            actual_df['value.timeReceived'] = actual_df['value.timeReceived'].astype(
                'datetime64[ns]')
            assert_frame_equal(expected_df.sort_values(['key.userId', 'value.time'])
                               .reset_index(drop=True),
                               actual_df.sort_values(['key.userId', 'value.time'])
                               .reset_index(drop=True), check_datetimelike_compat=True,
                               check_dtype=False)
"""