import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

import paramiko
from multiprocessing import Pool
from functools import partial
from datetime import datetime

logger = logging.getLogger(__name__)


class SftpDataReader():
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
        if "sftp_target_path" not in config_dict:
            now = datetime.now()
            current_time = now.strftime("%d-%m-%y::%H:%M:%S")
            self.root_dir = os.path.join("tmp", current_time)
        else:
            self.root_dir = config_dict["sftp_target_path"]

    def read_sftp_data(self) -> None:
        sftp_connection_args = {}
        sftp_source_path = self.config_dict.get('sftp_source_path')
        sftp_connection_args["username"] = self.config_dict.get('sftp_username')
        sftp_connection_args["host"] = self.config_dict.get('sftp_host')
        sftp_connection_args["private_key"] = self.config_dict.get('sftp_private_key')
        all_participants_ids = self._get_all_id_sftp(sftp_source_path,
                                                     sftp_connection_args)
        func = partial(self._fetch_data, self.root_dir, sftp_source_path,
                       self.variables, sftp_connection_args)
        with Pool(4) as p:
            p.map(func, all_participants_ids)
        logger.info(f"Data read from sftp and stored in {self.root_dir} folder")
        logger.info("To avoid redownloading, change config file to read from local")

    def get_root_dir(self) -> str:
        return self.root_dir

    def _fetch_data(self, root_path, sftp_source_path, included_var_cat,
                    sftp_connection_args, uid):
        with pysftp.Connection(**sftp_connection_args) as sftp:
            try:
                with sftp.cd(os.path.join(sftp_source_path, uid)):
                    source_folders = sftp.listdir(".")
                    for src in source_folders:
                        if self._is_src_in_category(src, included_var_cat):
                            dir_path = os.path.join(uid, src)
                            os.makedirs(os.path.join(root_path, dir_path),
                                        exist_ok=True)
                            try:
                                with sftp.cd(src):
                                    # To add new files only
                                    src_files = sftp.listdir(".")
                                    for src_file in src_files:
                                        # check if src file is a file or directory
                                        if sftp.isfile(src_file):
                                            sftp.get(src_file,
                                                     os.path.join(
                                                         root_path, dir_path, src_file),
                                                     preserve_mtime=True)
                                        else:
                                            if not os.path.exists(
                                                os.path.join(
                                                    root_path, dir_path, src_file
                                                )
                                            ):
                                                os.makedirs(
                                                    os.path.join(
                                                        root_path, dir_path,
                                                        src_file),
                                                    exist_ok=True)
                                                sftp.get_d(src_file,
                                                           os.path.join(
                                                               root_path,
                                                               dir_path,
                                                               src_file),
                                                           preserve_mtime=True)
                            except FileNotFoundError:
                                print("Folder not found: " + dir_path + "/" + src_file)
                                continue
                            except EOFError:
                                print("EOFError: " + dir_path + "/" + src_file)
                                continue
            except FileNotFoundError:
                print("Folder not found: " + uid)
                return

    def _is_src_in_category(self, src, categories):
        if categories == "all":
            return True
        for category in categories:
            if src[:len(category)] == category:
                return True
        return False

    def _get_all_id_sftp(self, sftp_source_path, sftp_connection_args):
        with pysftp.Connection(**sftp_connection_args) as sftp:
            with sftp.cd(sftp_source_path + '/'):
                return [x for x in sftp.listdir(sftp_source_path) if x[0] != "."]