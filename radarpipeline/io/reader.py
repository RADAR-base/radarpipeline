from typing import List, Tuple, Dict
from ..datalib import RadarFileData, RadarVariableData, RadarUserData, RadarData
from .dataReader import DataReader
import pandas as pd
import pysftp
from datetime import datetime
import os
import gzip
from tqdm import tqdm
class SFTPDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self._create_sftp_credentials()
        self.required_data = required_data

    def _create_sftp_credentials(self):
        if self.config["sftp_password"] is not None:
            self.sftp_cred =  {"host": self.config["sftp_host"], "username": self.config["sftp_username"], "password": self.config["sftp_password"]}
        elif self.config["sftp_private_key"] is not None:
            self.sftp_cred = {"host": self.config["sftp_host"], "username": self.config["sftp_username"], "private_key": self.config["sftp_private_key"]}
        self.source_dir = self.config["sftp_directory"]

    def read(self) -> RadarData:
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            uids = sftp.listdir()
            user_data_arr = {}
            for uid in tqdm(uids):
                if uid[0] == ".":
                    continue
                variable_data_arr = {}
                for  dirname in self.required_data:
                    file_data_arr = {}
                    if dirname not in sftp.listdir(f'{uid}/'):
                        continue
                    for data_file in sftp.listdir(f'{uid}/{dirname}/'):
                        if data_file.split(".")[-1] == "gz":
                            dt = data_file.split(".")[0]
                            if len(dt.split("_")) != 2:
                                continue
                        try:
                            file_data_arr[datetime.strptime(dt, '%Y%m%d_%H%M')] = RadarFileData(self._read_csv(sftp, f'{uid}/{dirname}/{data_file}'))
                        except pd.errors.EmptyDataError:
                            continue
                    if len(file_data_arr) > 0:
                        variable_data_arr[dirname] = RadarVariableData(file_data_arr)
                user_data_arr[uid] = variable_data_arr
        return RadarData(user_data_arr)

    def _read_csv(self, sftp, path):
        with sftp.open(path) as f:
            f.prefetch()
            gzip_fd = gzip.GzipFile(fileobj=f)
            df = pd.read_csv(gzip_fd)

class LocalDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self.required_data = required_data
        self.source_path = config.get('local_directory')

    def read(self) -> RadarData:
        user_data_arr = {}
        #check if source path directory exists
        if not os.path.isdir(self.source_path):
            raise Exception(f'{self.source_path} does not exist')

        for uid in os.listdir(self.source_path):
            variable_data_arr = {}
            for  dirname in self.required_data:
                file_data_arr = {}
                for data_file in os.listdir(f'{self.source_path}/{uid}/{dirname}/'):
                    if data_file.split(".")[-1] == "gz":
                        dt = data_file.split(".")[0]
                        if len(dt.split("_")) != 2:
                            continue
                    try:
                        file_data_arr[datetime.strptime(dt, '%Y%m%d_%H%M')] = RadarFileData(pd.read_csv(f'{self.source_path}/{uid}/{dirname}/{data_file}'))
                    except pd.errors.EmptyDataError:
                        continue
                if len(file_data_arr) > 0:
                    variable_data_arr[dirname] = RadarVariableData(file_data_arr)
            user_data_arr[uid] = variable_data_arr
        return RadarData(user_data_arr)