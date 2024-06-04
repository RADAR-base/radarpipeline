import unittest
from radarpipeline import radarpipeline, Project
from strictyaml.exceptions import YAMLValidationError
import pathlib as pl
import os
import pandas as pd
from pandas.testing import assert_frame_equal


class TestSampling(unittest.TestCase):

    def setUp(self):
        self.default_config = {
            'project': {
                'project_name': 'mock_project',
                'description': 'mock_description',
                'version': 'mock_version'},
            'input': {
                'source_type': 'mock',
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

    def get_config_output(self, config):
        project = Project(input_data=config)
        project.fetch_data()
        project.compute_features()
        output_data = project.features
        project.close_spark_session()
        return output_data

    def test_user_sampling_userid(self):
        user_sampling_config = self.default_config
        user_sampling_config['configurations']['user_sampling'] = {}
        user_sampling_config['configurations']['user_sampling']['method'] = 'userid'
        user_sampling_config['configurations']['user_sampling']['config'] = {
            'userids': ["2a02e53a-951e-4fd0-b47f-195a87096bd0"]}
        output_data = self.get_config_output(user_sampling_config)
        self.assertEqual(output_data['PhoneBatteryChargingDuration'][
            'key.userId'].unique(), ['2a02e53a-951e-4fd0-b47f-195a87096bd0'])
        self.assertEqual(output_data['StepCountPerDay']['key.userId'].unique(),
                         ['2a02e53a-951e-4fd0-b47f-195a87096bd0'])

    def test_user_sampling_count(self):
        user_sampling_config = self.default_config
        user_sampling_config['configurations']['user_sampling'] = {}
        user_sampling_config['configurations']['user_sampling']['method'] = 'count'
        user_sampling_config['configurations']['user_sampling']['config'] = {'count': 2}
        output_data = self.get_config_output(user_sampling_config)
        self.assertEqual(output_data['PhoneBatteryChargingDuration'][
            'key.userId'].unique().shape[0], 2)
        self.assertEqual(output_data['StepCountPerDay'][
            'key.userId'].unique().shape[0], 2)

    def test_user_sampling_fraction(self):
        user_sampling_config = self.default_config
        user_sampling_config['configurations']['user_sampling'] = {}
        user_sampling_config['configurations']['user_sampling']['method'] = 'fraction'
        user_sampling_config['configurations']['user_sampling'][
            'config'] = {'fraction': 0.75}
        output_data = self.get_config_output(user_sampling_config)
        self.assertEqual(output_data['PhoneBatteryChargingDuration'][
            'key.userId'].unique().shape[0], 3)
        self.assertEqual(output_data['StepCountPerDay'][
            'key.userId'].unique().shape[0], 3)

    def test_data_sampling_time(self):
        starttime = "2018-11-25 00:00:00"
        endtime = "2018-11-29 00:00:00"
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'time'
        data_sampling_config['configurations'][
            'data_sampling']['config'] = {
                'starttime': starttime,
                'endtime': endtime,
                'time_column': 'value.time'}
        output_data = self.get_config_output(data_sampling_config)
        self.assertGreaterEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].min(), pd.Timestamp(starttime).date())
        self.assertLessEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].max(), pd.Timestamp(endtime).date())
        self.assertGreaterEqual(output_data['StepCountPerDay'][
            'date'].min(), pd.Timestamp(starttime).date())
        self.assertLessEqual(output_data['StepCountPerDay'][
            'date'].max(), pd.Timestamp(endtime).date())

    def test_data_sampling_time_list(self):
        starttime = "2018-11-25 00:00:00"
        endtime = "2018-11-29 00:00:00"
        time_list = [{"starttime": starttime, "endtime": endtime,
                      "time_column": 'value.time'}]
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'time'
        data_sampling_config['configurations'][
            'data_sampling']['config'] = time_list
        output_data = self.get_config_output(data_sampling_config)
        self.assertGreaterEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].min(), pd.Timestamp(starttime).date())
        self.assertLessEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].max(), pd.Timestamp(endtime).date())
        self.assertGreaterEqual(output_data['StepCountPerDay'][
            'date'].min(), pd.Timestamp(starttime).date())
        self.assertLessEqual(output_data['StepCountPerDay'][
            'date'].max(), pd.Timestamp(endtime).date())

    def test_data_sampling_multiple_time(self):
        starttime_1 = "2018-11-25 00:00:00"
        endtime_1 = "2018-11-29 00:00:00"
        starttime_2 = "2019-01-01 00:00:00"
        endtime_2 = "2019-04-30 00:00:00"
        time_list = [{"starttime": starttime_1, "endtime": endtime_1,
                      "time_column": 'value.time'},
                     {"starttime": starttime_2, "endtime": endtime_2}]
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'time'
        data_sampling_config['configurations'][
            'data_sampling']['config'] = time_list
        output_data = self.get_config_output(data_sampling_config)
        self.assertGreaterEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].min(), pd.Timestamp(starttime_1).date())
        self.assertLessEqual(output_data['PhoneBatteryChargingDuration'][
            'date'].max(), pd.Timestamp(endtime_2).date())
        self.assertGreaterEqual(output_data['StepCountPerDay'][
            'date'].min(), pd.Timestamp(starttime_1).date())
        self.assertLessEqual(output_data['StepCountPerDay'][
            'date'].max(), pd.Timestamp(endtime_2).date())
        # check output_data['PhoneBatteryChargingDuration']['date'] is
        # between the time range
        # starttime_1 and endtime_1 and starttime_2 and endtime_2
        self.assertTrue(all((output_data['PhoneBatteryChargingDuration']['date']
                             >= pd.Timestamp(starttime_1).date())
                            & (output_data['PhoneBatteryChargingDuration']['date']
                               <= pd.Timestamp(endtime_1).date())
                            | (output_data['PhoneBatteryChargingDuration']['date']
                               >= pd.Timestamp(starttime_2).date())
                            & (output_data['PhoneBatteryChargingDuration']['date']
                               <= pd.Timestamp(endtime_2).date())))

        self.assertTrue(all((output_data['StepCountPerDay']['date']
                             >= pd.Timestamp(starttime_1).date())
                            & (output_data['StepCountPerDay']['date']
                               <= pd.Timestamp(endtime_1).date())
                            | (output_data['StepCountPerDay']['date']
                               >= pd.Timestamp(starttime_2).date())
                            & (output_data['StepCountPerDay']['date']
                               <= pd.Timestamp(endtime_2).date())))

    def test_data_sampling_multiple_time_single_starttime(self):
        starttime_1 = "2018-11-25 00:00:00"
        endtime_1 = "2018-11-29 00:00:00"
        starttime_2 = "2019-01-01 00:00:00"
        time_list = [{"starttime": starttime_1, "endtime": endtime_1,
                      "time_column": 'value.time'},
                     {"starttime": starttime_2}]
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'time'
        data_sampling_config['configurations'][
            'data_sampling']['config'] = time_list
        output_data = self.get_config_output(data_sampling_config)
        # check output_data['PhoneBatteryChargingDuration']['date'] is between
        # the time range
        # starttime_1 and endtime_1 and starttime_2
        self.assertTrue(all((output_data['PhoneBatteryChargingDuration']['date']
                             >= pd.Timestamp(starttime_1).date())
                            & (output_data['PhoneBatteryChargingDuration']['date']
                               <= pd.Timestamp(endtime_1).date())
                            | (output_data['PhoneBatteryChargingDuration']['date']
                               >= pd.Timestamp(starttime_2).date())))

        self.assertTrue(all((output_data['StepCountPerDay']['date']
                             >= pd.Timestamp(starttime_1).date())
                            & (output_data['StepCountPerDay']['date']
                               <= pd.Timestamp(endtime_1).date())
                            | (output_data['StepCountPerDay']['date']
                               >= pd.Timestamp(starttime_2).date())))

    def test_data_sampling_multiple_time_single_endtime(self):
        endtime_1 = "2018-11-29 00:00:00"
        starttime_2 = "2019-01-01 00:00:00"
        endtime_2 = "2019-04-30 00:00:00"
        time_list = [{"endtime": endtime_1,
                      "time_column": 'value.time'},
                     {"starttime": starttime_2, "endtime": endtime_2}]
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'time'
        data_sampling_config['configurations'][
            'data_sampling']['config'] = time_list
        output_data = self.get_config_output(data_sampling_config)
        # check output_data['PhoneBatteryChargingDuration']['date'] is between
        # the time range
        # endtime_1 and starttime_2 and endtime_2
        self.assertTrue(all((output_data['PhoneBatteryChargingDuration']['date']
                             <= pd.Timestamp(endtime_1).date())
                            | (output_data['PhoneBatteryChargingDuration']['date']
                               >= pd.Timestamp(starttime_2).date())
                            & (output_data['PhoneBatteryChargingDuration']['date']
                               <= pd.Timestamp(endtime_2).date())))

        self.assertTrue(all((output_data['StepCountPerDay']['date']
                             <= pd.Timestamp(endtime_1).date())
                            | (output_data['StepCountPerDay']['date']
                               >= pd.Timestamp(starttime_2).date())
                            & (output_data['StepCountPerDay']['date']
                               <= pd.Timestamp(endtime_2).date())))

    def test_data_sampling_count(self):
        data_sampling_config = self.default_config
        data_sampling_config['configurations']['data_sampling'] = {}
        data_sampling_config['configurations']['data_sampling']['method'] = 'count'
        data_sampling_config['configurations']['data_sampling']['config'] = {
            'count': 100
        }
        output_data = self.get_config_output(data_sampling_config)
        # check if count of per key.userId is less than or equal to 100
        self.assertTrue(all(output_data['PhoneBatteryChargingDuration'].groupby(
            'key.userId').size() <= 100))
        self.assertTrue(all(output_data['StepCountPerDay'].groupby(
            'key.userId').size() <= 100))

    def test_user_data_sampling(self):
        user_data_sampling_config = self.default_config
        user_data_sampling_config['configurations']['user_sampling'] = {}
        user_data_sampling_config['configurations']['user_sampling'][
            'method'] = 'userid'
        user_data_sampling_config['configurations']['user_sampling'][
            'config'] = {'userids': ["2a02e53a-951e-4fd0-b47f-195a87096bd0"]}
        user_data_sampling_config['configurations']['data_sampling'] = {}
        user_data_sampling_config['configurations']['data_sampling'][
            'method'] = 'count'
        user_data_sampling_config['configurations']['data_sampling'][
            'config'] = {'count': 100}
        output_data = self.get_config_output(user_data_sampling_config)
        self.assertEqual(output_data['PhoneBatteryChargingDuration'][
            'key.userId'].unique(), ['2a02e53a-951e-4fd0-b47f-195a87096bd0'])
        self.assertEqual(output_data['StepCountPerDay'][
            'key.userId'].unique(), ['2a02e53a-951e-4fd0-b47f-195a87096bd0'])
        self.assertTrue(all(output_data[
            'PhoneBatteryChargingDuration'].groupby('key.userId').size() <= 100))
        self.assertTrue(all(output_data[
            'StepCountPerDay'].groupby('key.userId').size() <= 100))
