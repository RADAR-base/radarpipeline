from radarpipeline.io.abc import Sampler
from typing import Any, Dict, List, Optional, Union
from random import sample
import pyspark.sql as ps
import logging


logger = logging.getLogger(__name__)


class UserSampler(Sampler):

    def __init__(self, config: Dict) -> None:
        super().__init__(config)

    def sample_uids(self, uid_list) -> None:
        if self.config['method'] == "fraction":
            fraction = self.config['config']['fraction']
            # sample fraction of the uids
            return self._sample_list(uid_list, round(len(uid_list) * fraction))
        elif self.config['method'] == "count":
            count = self.config['config']['count']
            # sample count of the uids
            return self._sample_list(uid_list, count)
        elif self.config['method'] == "userid":
            sampled_uids = self.config['config']['userids']
            for sampled_uid in sampled_uids:
                if sampled_uid not in uid_list:
                    logger.warning(f"User id {sampled_uid} not found in the data")
                    sampled_uids.remove(sampled_uid)
            return sampled_uids
        else:
            raise ValueError("Invalid method")

    def _sample_list(self, uid_list, number):
        return sample(uid_list, number)


class DataSampler(Sampler):
    def __init__(self, config: Dict) -> None:
        super().__init__(config)

    def sample_data(self, df: ps.DataFrame) -> Optional[ps.DataFrame]:
        if self.config['method'] == "fraction":
            fraction = self.config['config']['fraction']
            # sample fraction of the data
            return self._sample_data(df, fraction)
        elif self.config['method'] == "count":
            count = self.config['config']['count']
            # sample count of the data
            return self._sample_data(df, count / df.count())
        elif (self.config['method'] == "time"
              and not (type(self.config['config']) is list
                       and len(self.config['config']) != 1)):
            if len(self.config['config']) == 1:
                self.config['config'] = self.config['config'][0]
            starttime = self.config['config'].get('starttime', None)
            endtime = self.config['config'].get('endtime', None)
            time_col = self.config['config'].get('time_col', "value.time")
            # check if time_col is present in df
            if time_col not in df.columns:
                raise ValueError(f"Column {time_col} not found in the dataframe")
            return self._sample_data_by_time(df, starttime, endtime, time_col)
        elif self.config['method'] == "time" and type(self.config['config']) is list:
            sampled_dfs = []
            for time_config in self.config['config']:
                starttime = time_config.get('starttime', None)
                endtime = time_config.get('endtime', None)
                time_col = time_config.get('time_col', "value.time")
                # check if time_col is present in df
                if time_col not in df.columns:
                    raise ValueError(f"Column {time_col} not found in the dataframe")
                sampled_dfs.append(
                    self._sample_data_by_time(df, starttime, endtime, time_col))
            # combined sampled_dfs into one dataframe and deduplicate it
            return sampled_dfs[0].union(*sampled_dfs[1:]).distinct()
        else:
            raise ValueError("Invalid method")

    def _sample_data(self, df, fraction):
        return df.sample(fraction=fraction, withReplacement=True)

    def _sample_data_by_time(self, df, starttime, endtime, time_col):
        if endtime is None:
            return df.filter(df[f"`{time_col}`"] >= starttime)
        elif starttime is None:
            return df.filter(df[f"`{time_col}`"] < endtime)
        else:
            return df.filter(df[f"`{time_col}`"].between(starttime, endtime))
