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

# Testing SFTP connector

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


@pytest.yield_fixture
def content(sftpserver):
    with sftpserver.serve_content(deepcopy(CONTENT_OBJ)):
        yield


@pytest.mark.xfail(sys.version_info < (2, 7), reason="Intermittently broken on 2.6")
def test_sftpserver_bound(sftpserver):
    assert sftpserver.wait_for_bind(1)


def test_sftpserver_available(sftpserver):
    assert isinstance(sftpserver, SFTPServer)
    assert sftpserver.is_alive()
    print(sftpserver)
    assert str(sftpserver.port) in sftpserver.url


def test_connection_with(sftpserver):
    '''connect to a public sftp server'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        sftp.close()


def test_cd_path(sftpserver):
    '''test .cd with a path'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        home = sftp.pwd
        with sftp.cd('/home/test/pub'):
            assert sftp.pwd == '/home/test/pub'
        assert home == sftp.pwd
        sftp.close()


def test_cd_nested(sftpserver):
    '''test nested cd's'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        home = sftp.pwd
        with sftp.cd('/home'):
            assert sftp.pwd == '/home'
            with sftp.cd('test/pub'):
                assert sftp.pwd == '/home/test/pub'
            assert sftp.pwd == '/home'
        assert home == sftp.pwd
        sftp.close()


def test_listdir(sftpserver):
    '''test listdir'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub'):
            print(sftp.pwd)
            assert sftp.listdir() == ['foo1', 'foo2', 'make.txt']
        sftp.close()


def test_listdir_attr(sftpserver):
    '''test listdir'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub'):
            attrs = sftp._sftp.listdir_attr()
            assert len(attrs) == 3
            # test they are in filename order
            assert attrs[0].filename == 'make.txt'
            assert attrs[1].filename == 'foo1'
            assert attrs[2].filename == 'foo2'
            # test that longname is there
            for attr in attrs:
                assert attr.longname is not None
        sftp.close()


def test_isfile(sftpserver):
    '''test .isfile() functionality'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        rfile = '/home/test/pub/make.txt'
        rdir = 'pub'
        assert sftp.isfile(rfile)
        assert sftp.isfile(rdir) is False
        sftp.close()


def test_get(sftpserver):
    '''download a file'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub/foo1'):
            with tempfile_containing('') as fname:
                sftp.get('foo1.txt', fname)
                assert open(fname, 'rb').read() == b'content of foo1.txt'
        sftp.close()


def test_get_preserve_mtime(sftpserver):
    '''test that m_time is preserved from local to remote, when get'''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub/foo1'):
            rfile = 'foo1.txt'
            with tempfile_containing('') as localfile:
                r_stat = sftp._sftp.stat(rfile)
                sftp.get(rfile, localfile, preserve_mtime=True)
                assert r_stat.st_mtime == os.stat(localfile).st_mtime
        sftp.close()


def test_get_d(sftpserver):
    '''test the get_d for remotepath is pwd '.' '''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub/'):
            localpath = mkdtemp()
            sftp.get_d('.', localpath)
            checks = [(['', ], ['make.txt', ]), ]
            for pth, fls in checks:
                assert sorted(os.listdir(os.path.join(localpath, *pth))) == fls
            # cleanup local
            shutil.rmtree(localpath)
        sftp.close()


def test_get_d_pathed(sftpserver):
    '''test the get_d for localpath, starting deeper then pwd '''
    with sftpserver.serve_content(CONTENT_OBJ):
        sftp = SftpConnector(conn(sftpserver), ['x'])
        sftp.connect()
        with sftp.cd('/home/test/pub/'):
            localpath = mkdtemp()
            sftp.get_d('foo1', localpath)

            chex = [(['', ],
                     ['foo1.txt', 'image01.jpg']), ]
            for pth, fls in chex:
                assert sorted(os.listdir(os.path.join(localpath, *pth))) == fls

            # cleanup local
            shutil.rmtree(localpath)
        sftp.close()
