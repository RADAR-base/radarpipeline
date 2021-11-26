from typing import List, Tuple, Dict
from ..datalib import Data
from . import DataReader
import pandas as pd
import pysftp

class SFTPDataReaderCSV(DataReader):
    def __init__(self, config: Dict, required_data: List[str]) -> None:
        super().__init__(config)
        self._create_sftp_credentials()
        self.required_data = required_data

    def _create_sftp_credentials(self):
        if self.config["sftp_password"] is not None:
            self.sftp_cred =  {"hostname": self.config["sftp_host"], "username": self.config["sftp_username"], "password": self.config["sftp_password"]}
        elif self.config["sftp_private_key"] is not None:
            self.sftp_cred = {"hostname": self.config["sftp_host"], "username": self.config["sftp_username"], "private_key": self.config["sftp_private_key"]}
        self.source_dir = self.config["sftp_directory"]

    def read(self) -> Data:
        data = Data()
        with pysftp.Connection(**self.sftp_cred) as sftp:
            sftp.cwd(self.source_dir)
            uids = sftp.listdir()
            for uid in uids:
                for  dirname in self.required_data:
                    for data_file in sftp.listdir(f'{uid}/{dirname}/'):
                        if data_file.split(".")[-1] == "gz":
                            dt = data_file.split(".")[0]
                            if len(dt.split("_")) != 2:
                                continue
                        try:
                            df = pd.read_csv(f'{uid}/{dirname}/{data_file}')
                        except pd.errors.EmptyDataError:
                            continue
        return data

class LocalDataReaderCSV(DataReader):
    def __init__(self, config: Dict) -> None:
        self.config = config

    def read(self) -> Data:
        pass