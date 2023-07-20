import unittest
from pytest_sftpserver.sftp.server import SFTPServer
import pytest
from paramiko import Transport
from paramiko.channel import Channel
from paramiko.sftp_client import SFTPClient
import sys
import os

from copy import deepcopy
from radarpipeline.io.downloader import SftpDataReader
from dhp.test import tempfile_containing
import shutil
from tempfile import mkdtemp

STARS8192 = '*' * 8192
TEST_PATH_DIR = "tests/resources/sftp_test"

CONTENT_OBJ = {
    'mock_radar_sftp_data': {
        '5c0e2ec7-6f85-4041-9669-7145075d1754': {
            'android_phone_battery_level': {
                '20181025_1200.csv.gz': "Mock CSV",
                '20181025_1300.csv.gz': "Mock CSV",
                'schema-android_phone_battery_level.json': "Mock JSON"
            },
            'android_phone_step_count': {
                '20181025_1200.csv.gz': "Mock CSV",
                '20181025_1300.csv.gz': "Mock CSV",
                'schema-android_phone_battery_level.json': "Mock JSON"
            }
        },
        '2a02e53a-951e-4fd0-b47f-195a87096bd0': {
            'android_phone_acceleration': {
                '20181127_0900.csv.gz': "Mock CSV",
                '20181127_1000.csv.gz': "Mock CSV",
                'schema-android_phone_acceleration.json': "Mock JSON"
            },
            'android_phone_battery_level': {
                '20181123_1500.csv.gz': "Mock CSV",
                '20181123_1600.csv.gz': "Mock CSV",
                'schema-android_phone_battery_level.json': "Mock JSON"
            },
            'android_phone_step_count': {
                '20181123_1500.csv.gz': "Mock CSV",
                '20181123_1700.csv.gz': "Mock CSV",
                'schema-android_phone_step_count.json': "Mock JSON"
            }
        }
    }
}


def conn(sftpsrv):
    """return a dictionary holding argument info for the pysftp client"""
    return {'sftp_host': sftpsrv.host, 'sftp_port': sftpsrv.port,
            'sftp_username': 'user',
            'sftp_password': 'pw', 'sftp_source_path': 'mock_radar_sftp_data',
            "sftp_target_path": TEST_PATH_DIR}


@pytest.yield_fixture(scope="session")
def sftpclient(sftpserver):
    transport = Transport((sftpserver.host, sftpserver.port))
    transport.connect(username="a", password="b")
    sftpclient = SFTPClient.from_transport(transport)
    yield sftpclient
    sftpclient.close()
    transport.close()


class TestDownloader(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_fixture(self, sftpserver):
        self.sftpserver = sftpserver

    def setUp(self):
        self.variables = ['android_phone_battery_level', 'android_phone_step_count']
        self.config_dict = conn(self.sftpserver)
        self.target_path = TEST_PATH_DIR
        self.source_path = "mock_radar_sftp_data"

    def test_get_root_dir(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            sftp_data_reader = SftpDataReader(self.config_dict, self.variables)
            root_dir = sftp_data_reader.get_root_dir()
            self.assertEqual(root_dir, self.target_path)

    def test_get_all_ids(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            sftp_data_reader = SftpDataReader(self.config_dict, self.variables)
            ids = sftp_data_reader._get_all_id_sftp(self.source_path)
            expected_ids = ['5c0e2ec7-6f85-4041-9669-7145075d1754',
                            '2a02e53a-951e-4fd0-b47f-195a87096bd0']
            # assert list equal
            self.assertListEqual(sorted(ids), sorted(expected_ids))

    def test_read_sftp_data(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            sftp_data_reader = SftpDataReader(self.config_dict, self.variables)
            sftp_data_reader.read_sftp_data()
        # check if the files are downloaded in the target path
        uids = ['5c0e2ec7-6f85-4041-9669-7145075d1754',
                '2a02e53a-951e-4fd0-b47f-195a87096bd0']
        for uid in uids:
            self.assertTrue(os.path.exists(os.path.join(self.target_path, uid)))
            for variable in self.variables:
                self.assertTrue(os.path.exists(os.path.join(self.target_path,
                                                            uid, variable)))

    def test_read_sftp_data_with_one_variable(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            variables = ['android_phone_acceleration']
            sftp_data_reader = SftpDataReader(self.config_dict, variables)
            sftp_data_reader.read_sftp_data()
            uid_correct = '2a02e53a-951e-4fd0-b47f-195a87096bd0'
            self.assertTrue(os.path.exists(os.path.join(self.target_path, uid_correct)))
            for variable in variables:
                self.assertTrue(os.path.exists(os.path.join(self.target_path,
                                                            uid_correct, variable)))
            uid_incorrect = '5c0e2ec7-6f85-4041-9669-7145075d1754'
            self.assertFalse(os.path.exists(os.path.join(self.target_path,
                                                         uid_incorrect)))

    def test_read_sftp_data_with_invalid_config(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            variables = ['invalid']
            sftp_data_reader = SftpDataReader(self.config_dict, variables)
            sftp_data_reader.read_sftp_data()
            uids = ['5c0e2ec7-6f85-4041-9669-7145075d1754',
                    '2a02e53a-951e-4fd0-b47f-195a87096bd0']
            for uid in uids:
                self.assertFalse(os.path.exists(os.path.join(self.target_path, uid)))

    def test_fetch_data(self):
        with self.sftpserver.serve_content(CONTENT_OBJ):
            sftp_data_reader = SftpDataReader(self.config_dict, self.variables)
            root_dir = sftp_data_reader.get_root_dir()
            uid = '5c0e2ec7-6f85-4041-9669-7145075d1754'
            sftp_data_reader._fetch_data(root_dir, self.source_path,
                                         self.variables, uid)
        # check if the files are downloaded in the target path
        self.assertTrue(os.path.exists(os.path.join(self.target_path, uid)))
        for variable in self.variables:
            self.assertTrue(os.path.exists(
                os.path.join(self.target_path, uid, variable)))

    def tearDown(self):
        # if self.target_path exists, remove it
        if os.path.exists(self.target_path):
            shutil.rmtree(self.target_path)
