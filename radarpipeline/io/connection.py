import paramiko
from paramiko import AgentKey, RSAKey, DSSKey
from paramiko import SSHException, AuthenticationException

from typing import Any, Dict, List, Optional, Union
import json
import logging
import os
from radarpipeline.common.utils import reparent

from contextlib import contextmanager
from stat import S_IMODE, S_ISDIR, S_ISREG
import warnings

logger = logging.getLogger(__name__)


class ConnectionException(Exception):
    """Exception raised for connection problems

    Attributes:
        message  -- explanation of the error
    """

    def __init__(self, host, port):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, host, port)
        self.message = 'Could not connect to host:port.  %s:%s'


class CredentialException(Exception):
    """Exception raised for credential problems

    Attributes:
        message  -- explanation of the error
    """

    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)
        self.message = message


class HostKeysException(Exception):
    '''raised when a problem with HostKeys is encountered'''
    pass


class SftpConnector():
    """
    Class for reading data from SFTP
    """
    def __init__(self, config_dict: Dict, variables: List[str]):
        """
        Parameters
        ----------
        config_dict : Dict
            Dictionary containing configuration for SFTP
        variables : List
            List of variables to read
        root_dir : str, optional
        """

        self.config_dict = config_dict
        self.variables = variables

    def connect(self):
        """
        Connect to SFTP server
        """
        username = self.config_dict.get('sftp_username')
        host = self.config_dict.get('sftp_host')
        password = self.config_dict.get('sftp_password')
        port = self.config_dict.get('sftp_port', 22)
        private_key = self.config_dict.get('sftp_private_key')
        self._tconnect = {'username': username, 'password': password, 'hostkey': None,
                          'pkey': None}
        self._sftp_live = False
        self._transport = None
        self._transport = paramiko.Transport((host, port))
        self._transport.use_compression(False)
        private_key_pass = None
        self._set_authentication(password, private_key, private_key_pass)
        self._transport.connect(**self._tconnect)
        self._sftp_connect()

    def _set_authentication(self, password, private_key, private_key_pass):
        '''Authenticate the transport. prefer password if given'''
        if password is None:
            # Use Private Key.
            if not private_key:
                # Try to use default key.
                if os.path.exists(os.path.expanduser('~/.ssh/id_rsa')):
                    private_key = '~/.ssh/id_rsa'
                elif os.path.exists(os.path.expanduser('~/.ssh/id_dsa')):
                    private_key = '~/.ssh/id_dsa'
                else:
                    raise CredentialException("No password or key specified.")

            if isinstance(private_key, (AgentKey, RSAKey)):
                # use the paramiko agent or rsa key
                self._tconnect['pkey'] = private_key
            else:
                # isn't a paramiko AgentKey or RSAKey, try to build a
                # key from what we assume is a path to a key
                private_key_file = os.path.expanduser(private_key)
                try:  # try rsa
                    self._tconnect['pkey'] = RSAKey.from_private_key_file(
                        private_key_file, private_key_pass)
                except paramiko.SSHException:   # if it fails, try dss
                    # pylint:disable=r0204
                    self._tconnect['pkey'] = DSSKey.from_private_key_file(
                        private_key_file, private_key_pass)

    def _sftp_connect(self):
        """Establish the SFTP connection."""
        if not self._sftp_live:
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._sftp_live = True

    @property
    def pwd(self):
        '''return the current working directory

        :returns: (str) current working directory

        '''
        self._sftp_connect()
        return self._sftp.normalize('.')

    @contextmanager
    def cd(self, remotepath=None):  # pylint:disable=c0103
        """context manager that can change to a optionally specified remote
        directory and restores the old pwd on exit.

        :param str|None remotepath: *Default: None* -
            remotepath to temporarily make the current directory
        :returns: None
        :raises: IOError, if remote path doesn't exist
        """
        original_path = self.pwd
        try:
            if remotepath is not None:
                self.chdir(remotepath)
            yield
        finally:
            self.chdir(original_path)

    def chdir(self, remotepath):
        """change the current working directory on the remote

        :param str remotepath: the remote path to change to

        :returns: None

        :raises: IOError, if path does not exist

        """
        self._sftp_connect()
        self._sftp.chdir(remotepath)

    def listdir(self, remotepath='.'):
        """return a list of files/directories for the given remote path.
        Unlike, paramiko, the directory listing is sorted.

        :param str remotepath: path to list on the server

        :returns: (list of str) directory entries, sorted

        """
        self._sftp_connect()
        return sorted(self._sftp.listdir(remotepath))

    def isfile(self, remotepath):
        """return true if remotepath is a file

        :param str remotepath: the path to test

        :returns: (bool)

        """
        self._sftp_connect()
        try:
            result = S_ISREG(self._sftp.stat(remotepath).st_mode)
        except IOError:     # no such file
            result = False
        return result

    def get(self, remotepath, localpath=None, callback=None,
            preserve_mtime=False):
        """Copies a file between the remote host and the local host.

        :param str remotepath: the remote path and filename, source
        :param str localpath:
            the local path and filename to copy, destination. If not specified,
            file is copied to local current working directory
        :param callable callback:
            optional callback function (form: ``func(int, int)``) that accepts
            the bytes transferred so far and the total bytes to be transferred.
        :param bool preserve_mtime:
            *Default: False* - make the modification time(st_mtime) on the
            local file match the time on the remote. (st_atime can differ
            because stat'ing the localfile can/does update it's st_atime)

        :returns: None

        :raises: IOError

        """
        if not localpath:
            localpath = os.path.split(remotepath)[1]

        self._sftp_connect()
        if preserve_mtime:
            sftpattrs = self._sftp.stat(remotepath)

        self._sftp.get(remotepath, localpath, callback=callback)
        if preserve_mtime:
            os.utime(localpath, (sftpattrs.st_atime, sftpattrs.st_mtime))

    def get_d(self, remotedir, localdir, preserve_mtime=False):
        """get the contents of remotedir and write to locadir. (non-recursive)

        :param str remotedir: the remote directory to copy from (source)
        :param str localdir: the local directory to copy to (target)
        :param bool preserve_mtime: *Default: False* -
            preserve modification time on files

        :returns: None

        :raises:
        """
        self._sftp_connect()
        with self.cd(remotedir):
            for sattr in self._sftp.listdir_attr('.'):
                if S_ISREG(sattr.st_mode):
                    rname = sattr.filename
                    self.get(rname, reparent(localdir, rname),
                             preserve_mtime=preserve_mtime)

    def close(self):
        """Closes the connection and cleans up."""
        # Close SFTP Connection.
        if self._sftp_live:
            self._sftp.close()
            self._sftp_live = False
        # Close the SSH Transport.
        if self._transport:
            self._transport.close()
            self._transport = None
