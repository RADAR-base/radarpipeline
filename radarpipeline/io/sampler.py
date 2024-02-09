from radarpipeline.io.abc import Sampler
from typing import Any, Dict, List, Optional, Union
from random import sample
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
