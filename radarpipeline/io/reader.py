from typing import List, Tuple, Dict
from ..datalib import RadarFileData, RadarVariableData, RadarUserData, RadarData
from .dataReader import DataReader
import pandas as pd
import pysftp
from datetime import datetime
import os
import gzip
from tqdm import tqdm
from multiprocessing.pool import Pool
from functools import partialmethod
from itertools import repeat
import gc


class SFTPDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self._create_sftp_credentials()
        self.required_data = required_data

    def _create_sftp_credentials(self):
        if self.config["sftp_password"] is not None:
            self.sftp_cred = {
                "host": self.config["sftp_host"], "username": self.config["sftp_username"], "password": self.config["sftp_password"]}
        elif self.config["sftp_private_key"] is not None:
            self.sftp_cred = {
                "host": self.config["sftp_host"], "username": self.config["sftp_username"], "private_key": self.config["sftp_private_key"]}
        self.source_dir = self.config["sftp_directory"]

    # Python code to convert list of tuple into dictionary

    def _tuples_to_dict(self, tuple_list):
        di = dict()
        for element in tuple_list:
            if element is not None:
                a, b = element
                di[a] = b
        return di

    def read(self) -> RadarData:
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            uids = sftp.listdir()
            user_data_arr = {}
            for uid in uids:
                if uid[0] == ".":
                    continue
                variable_data_arr = {}
                for dirname in self.required_data:
                    file_data_arr = {}
                    if dirname not in sftp.listdir(f'{uid}/'):
                        continue
                    #func = partial(self._read_data_file, sftp, f'{uid}/{dirname}')
                    data_files = sftp.listdir(f'{uid}/{dirname}/')
                    # read files paraller using pool
                    with Pool(8) as p:
                        file_data_arr = self._tuples_to_dict(
                            p.starmap(self._read_data_file, zip(repeat(f'{uid}/{dirname}'), data_files)))
                    if len(file_data_arr) > 0:
                        variable_data_arr[dirname] = RadarVariableData(
                            file_data_arr)
                user_data_arr[uid] = RadarUserData(variable_data_arr)
        return RadarData(user_data_arr)

    def _read_data_file(self, dirs, data_file):
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            if data_file.split(".")[-1] == "gz":
                dt = data_file.split(".")[0]
                if len(dt.split("_")) != 2:
                    return (None, None)
                try:
                    return (datetime.strptime(dt, '%Y%m%d_%H%M'), RadarFileData(self._read_csv(sftp, f'{dirs}/{data_file}')))
                except pd.errors.EmptyDataError:
                    return (None, None)

    def _read_csv(self, sftp, path):
        with sftp.open(path) as f:
            f.prefetch()
            gzip_fd = gzip.GzipFile(fileobj=f)
            df = pd.read_csv(gzip_fd)
        return df


class LocalDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self.required_data = required_data
        self.source_path = config.get('local_directory')

    def read(self) -> RadarData:
        user_data_arr = {}
        # check if source path directory exists
        if not os.path.isdir(self.source_path):
            raise Exception(f'{self.source_path} does not exist')

        for uid in tqdm(os.listdir(self.source_path)):
            if uid[0] == ".":
                continue
            variable_data_arr = {}
            for dirname in self.required_data:
                file_data_arr = {}
                if dirname not in os.listdir(f'{self.source_path}/{uid}/'):
                    continue
                data_files = os.listdir(f'{self.source_path}/{uid}/{dirname}/')
                # read files paraller using pool
                with Pool(8) as p:
                    file_data_arr = self._tuples_to_dict(p.starmap(self._read_data_file, zip(
                        repeat(f'{self.source_path}/{uid}/{dirname}'), data_files)))
                if len(file_data_arr) > 0:
                    variable_data_arr[dirname] = RadarVariableData(
                        file_data_arr)
            user_data_arr[uid] = RadarUserData(variable_data_arr)
        return RadarData(user_data_arr)

    def _read_data_file(self, dirs, data_file):
        if data_file.split(".")[-1] == "gz":
            dt = data_file.split(".")[0]
            if len(dt.split("_")) != 2:
                return (None, None)
            try:
                return (datetime.strptime(dt, '%Y%m%d_%H%M'), RadarFileData(self._read_csv(f'{dirs}/{data_file}')))
            except pd.errors.EmptyDataError:
                return (None, None)

    def _read_csv(self, path):
        df = pd.read_csv(path)
        return df

    def _tuples_to_dict(self, tuple_list):
        di = dict()
        for element in tuple_list:
            if element is not None:
                a, b = element
                di[a] = b
        return di
