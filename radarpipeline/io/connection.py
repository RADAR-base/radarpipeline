import paramiko
from paramiko import AgentKey, RSAKey, DSSKey
from paramiko import SSHException, AuthenticationException

from typing import Any, Dict, List, Optional, Union
import json
import logging
import os
from radarpipeline.common.utils import reparent
import posixpath

from contextlib import contextmanager
from stat import S_IMODE, S_ISDIR, S_ISREG
import warnings

logger = logging.getLogger(__name__)


class WTCallbacks(object):
    '''an object to house the callbacks, used internally'''
    def __init__(self):
        '''set instance vars'''
        self._flist = []
        self._dlist = []
        self._ulist = []

    def file_cb(self, pathname):
        '''called for regular files, appends pathname to .flist

        :param str pathname: file path
        '''
        self._flist.append(pathname)

    def dir_cb(self, pathname):
        '''called for directories, appends pathname to .dlist

        :param str pathname: directory path
        '''
        self._dlist.append(pathname)

    def unk_cb(self, pathname):
        '''called for unknown file types, appends pathname to .ulist

        :param str pathname: unknown entity path
        '''
        self._ulist.append(pathname)

    @property
    def flist(self):
        '''return a sorted list of files currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._flist)

    @flist.setter
    def flist(self, val):
        '''setter for _flist '''
        self._flist = val

    @property
    def dlist(self):
        '''return a sorted list of directories currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._dlist)

    @dlist.setter
    def dlist(self, val):
        '''setter for _dlist '''
        self._dlist = val

    @property
    def ulist(self):
        '''return a sorted list of unknown entities currently traversed

        :getter: returns the list
        :setter: sets the list
        :type: list
        '''
        return sorted(self._ulist)

    @ulist.setter
    def ulist(self, val):
        '''setter for _ulist '''
        self._ulist = val


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

    def get_r(self, remotedir, localdir, preserve_mtime=False):
        """recursively copy remotedir structure to localdir

        :param str remotedir: the remote directory to copy from
        :param str localdir: the local directory to copy to
        :param bool preserve_mtime: *Default: False* -
            preserve modification time on files

        :returns: None

        :raises:

        """
        self._sftp_connect()
        wtcb = WTCallbacks()
        self.walktree(remotedir, wtcb.file_cb, wtcb.dir_cb, wtcb.unk_cb)
        # handle directories we recursed through
        for dname in wtcb.dlist:
            for subdir in path_advance(dname):
                try:
                    os.mkdir(reparent(localdir, subdir))
                    # force result to a list for setter,
                    wtcb.dlist = wtcb.dlist + [subdir, ]
                except OSError:     # dir exists
                    pass

        for fname in wtcb.flist:
            # they may have told us to start down farther, so we may not have
            # recursed through some, ensure local dir structure matches
            head, _ = os.path.split(fname)
            if head not in wtcb.dlist:
                for subdir in path_advance(head):
                    if subdir not in wtcb.dlist and subdir != '.':
                        os.mkdir(reparent(localdir, subdir))
                        wtcb.dlist = wtcb.dlist + [subdir, ]

            self.get(fname,
                     reparent(localdir, fname),
                     preserve_mtime=preserve_mtime)

    def walktree(self, remotepath, fcallback, dcallback, ucallback,
                 recurse=True):
        '''recursively descend, depth first, the directory tree rooted at
        remotepath, calling discreet callback functions for each regular file,
        directory and unknown file type.

        :param str remotepath:
            root of remote directory to descend, use '.' to start at
            :attr:`.pwd`
        :param callable fcallback:
            callback function to invoke for a regular file.
            (form: ``func(str)``)
        :param callable dcallback:
            callback function to invoke for a directory. (form: ``func(str)``)
        :param callable ucallback:
            callback function to invoke for an unknown file type.
            (form: ``func(str)``)
        :param bool recurse: *Default: True* - should it recurse

        :returns: None

        :raises:

        '''
        self._sftp_connect()
        for entry in self.listdir(remotepath):
            pathname = posixpath.join(remotepath, entry)
            mode = self._sftp.stat(pathname).st_mode
            if S_ISDIR(mode):
                # It's a directory, call the dcallback function
                dcallback(pathname)
                if recurse:
                    # now, recurse into it
                    self.walktree(pathname, fcallback, dcallback, ucallback)
            elif S_ISREG(mode):
                # It's a file, call the fcallback function
                fcallback(pathname)
            else:
                # Unknown file type
                ucallback(pathname)

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


def path_advance(thepath, sep=os.sep):
    '''generator to iterate over a file path forwards

    :param str thepath: the path to navigate forwards
    :param str sep: *Default: os.sep* - the path separator to use

    :returns: (iter)able of strings

    '''
    # handle a direct path
    pre = ''
    if thepath[0] == sep:
        pre = sep
    curpath = ''
    parts = thepath.split(sep)
    if pre:
        if parts[0]:
            parts[0] = pre + parts[0]
        else:
            parts[1] = pre + parts[1]
    for part in parts:
        curpath = os.path.join(curpath, part)
        if curpath:
            yield curpath
