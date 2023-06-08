import unittest
from pytest_sftpserver.sftp.server import SFTPServer
import pytest
from paramiko import Transport
from paramiko.channel import Channel
from paramiko.sftp_client import SFTPClient
import sys

from copy import deepcopy
from radarpipeline.io.connection import SftpConnector

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
