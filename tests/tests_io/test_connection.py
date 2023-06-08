import unittest
from pytest_sftpserver.sftp.server import SFTPServer
import pytest
from paramiko import Transport
from paramiko.channel import Channel
from paramiko.sftp_client import SFTPClient
import sys
import os

from copy import deepcopy
from radarpipeline.io.connection import SftpConnector
from dhp.test import tempfile_containing
import shutil
from tempfile import mkdtemp

STARS8192 = '*' * 8192
TEST_PATH_DIR = "tests/resources/sftp_test"

CONTENT_OBJ = {
    'home': {
        'test': {
            'pub': {
                'make.txt': "content of make.txt",
                'foo1': {
                    'foo1.txt': 'content of foo1.txt',
                    'image01.jpg': 'data for image01.jpg'
                },
                'foo2': {
                    'foo2.txt': 'content of foo2.txt',
                    'bar1': {
                        'bar1.txt': 'contents bar1.txt'
                    }
                }
            },
            'read.me': 'contents of read.me',
        }
    }
}


def conn(sftpsrv):
    """return a dictionary holding argument info for the pysftp client"""
    return {'sftp_host': sftpsrv.host, 'sftp_port': sftpsrv.port,
            'sftp_username': 'user',
            'sftp_password': 'pw', 'default_path': '/home/test'}


@pytest.yield_fixture(scope="session")
def sftpclient(sftpserver):
    transport = Transport((sftpserver.host, sftpserver.port))
    transport.connect(username="a", password="b")
    sftpclient = SFTPClient.from_transport(transport)
    yield sftpclient
    sftpclient.close()
    transport.close()


class TestConnection(unittest.TestCase):

    DIR_LIST = [('pub', ),
                ('pub', 'foo1'),
                ('pub', 'foo2'),
                ('pub', 'foo2', 'bar1')]
    FILE_LIST = [('pub', 'read.me'),
                 ('pub', 'make.txt'),
                 ('pub', 'foo1', 'foo1.txt'),
                 ('pub', 'foo2', 'foo2.txt'),
                 ('pub', 'foo2', 'bar1', 'bar1.txt')]
    TEST_PATH_DIR = "./tests/resources/sftp_test"
    STARS8192 = '*' * 8192

    @pytest.fixture(autouse=True)
    def prepare_fixture(self, sftpserver):
        self.sftpserver = sftpserver

    @staticmethod
    def build_dir_struct(cls, local_path):
        '''build directory structure'''
        for dparts in cls.DIR_LIST:
            os.mkdir(os.path.join(local_path, *dparts))
        for fparts in cls.FILE_LIST:
            with open(os.path.join(local_path, *fparts), 'wb') as fhndl:
                try:
                    fhndl.write(STARS8192)
                except TypeError:
                    fhndl.write(bytes(STARS8192, 'UTF-8'))

    @staticmethod
    def remove_dir_struct(cls, local_path):
        '''clean up directory struct'''
        for fparts in cls.FILE_LIST:
            os.remove(os.path.join(local_path, *fparts))
        for dparts in reversed(cls.DIR_LIST):
            os.rmdir(os.path.join(local_path, *dparts))

    @classmethod
    def setUpClass(cls):
        # create TEST_PATH_DIR if it does not exist
        if not os.path.exists(TEST_PATH_DIR):
            os.mkdir(TEST_PATH_DIR)
        cls.build_dir_struct(cls, TEST_PATH_DIR)

    @classmethod
    def tearDownClass(cls):
        cls.remove_dir_struct(cls, TEST_PATH_DIR)
        # Delete TEST_PATH_DIR if it is empty
        if not os.listdir(TEST_PATH_DIR):
            os.rmdir(TEST_PATH_DIR)

    def setUp(self):
        self.sftp = SftpConnector(conn(self.sftpserver), ['x'])
        self.home = "/home/test"

    def tearDown(self):
        self.sftp.close()

    def test_connection_with(self):
        '''connect to a public self.sftp server'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()

    def test_cd_path(self):
        '''test .cd with a path'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            home = self.sftp.pwd
            with self.sftp.cd(f'{self.home}/pub'):
                assert self.sftp.pwd == f'{self.home}/pub'
            assert home == self.sftp.pwd

    def test_cd_nested(self):
        '''test nested cd's'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            home = self.sftp.pwd
            with self.sftp.cd('/home'):
                assert self.sftp.pwd == '/home'
                with self.sftp.cd('test/pub'):
                    assert self.sftp.pwd == f'{self.home}/pub'
                assert self.sftp.pwd == '/home'
            assert home == self.sftp.pwd

    def test_listdir(self):
        '''test listdir'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub'):
                assert self.sftp.listdir() == ['foo1', 'foo2', 'make.txt']
            self.sftp.close()

    def test_listdir_attr(self):
        '''test listdir'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub'):
                attrs = self.sftp._sftp.listdir_attr()
                assert len(attrs) == 3
                # test they are in filename order
                assert attrs[0].filename == 'make.txt'
                assert attrs[1].filename == 'foo1'
                assert attrs[2].filename == 'foo2'
                # test that longname is there
                for attr in attrs:
                    assert attr.longname is not None
            self.sftp.close()

    def test_isfile(self):

        '''test .isfile() functionality'''

        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            rfile = f'{self.home}/pub/make.txt'
            rdir = 'pub'
            assert self.sftp.isfile(rfile)
            assert self.sftp.isfile(rdir) is False
            self.sftp.close()

    def test_get(self):
        '''download a file'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub/foo1'):
                with tempfile_containing('') as fname:
                    self.sftp.get('foo1.txt', fname)
                    assert open(fname, 'rb').read() == b'content of foo1.txt'
            self.sftp.close()

    def test_get_preserve_mtime(self):
        '''test that m_time is preserved from local to remote, when get'''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub/foo1'):
                rfile = 'foo1.txt'
                with tempfile_containing('') as localfile:
                    r_stat = self.sftp._sftp.stat(rfile)
                    self.sftp.get(rfile, localfile, preserve_mtime=True)
                    assert r_stat.st_mtime == os.stat(localfile).st_mtime
            self.sftp.close()

    def test_get_d(self):
        '''test the get_d for remotepath is pwd '.' '''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub/'):
                localpath = mkdtemp()
                self.sftp.get_d('.', localpath)
                checks = [(['', ], ['make.txt', ]), ]
                for pth, fls in checks:
                    assert sorted(os.listdir(os.path.join(localpath, *pth))) == fls
                # cleanup local
                shutil.rmtree(localpath)
            self.sftp.close()

    def test_get_d_pathed(self):
        '''test the get_d for localpath, starting deeper then pwd '''
        with self.sftpserver.serve_content(CONTENT_OBJ):
            self.sftp.connect()
            with self.sftp.cd(f'{self.home}/pub/'):
                localpath = mkdtemp()
                self.sftp.get_d('foo1', localpath)

                chex = [(['', ],
                         ['foo1.txt', 'image01.jpg']), ]
                for pth, fls in chex:
                    assert sorted(os.listdir(os.path.join(localpath, *pth))) == fls

                # cleanup local
                shutil.rmtree(localpath)
            self.sftp.close()
