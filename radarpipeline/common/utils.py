import os
from functools import reduce
from typing import Any, Dict, List

import pyspark.sql as ps
import yaml


def read_yaml(yaml_file_path: str) -> Dict[str, Any]:
    """
    Read yaml file and return a dict of the content

    Parameters
    ----------
    yaml_file_path : str
        Path to yaml file

    Returns
    -------
    Dict[str, Any]
        Dict of the content of the yaml file

    Raises
    ------
    ValueError
        If yaml file is not found or is not a yaml file, or is empty
    """

    if not os.path.exists(yaml_file_path):
        raise ValueError("Input file does not exist ")
    if not yaml_file_path.lower().endswith(".yaml"):
        raise ValueError("Input file is not a yaml file")
    if os.stat(yaml_file_path).st_size == 0:
        raise ValueError("Input file is empty")

    with open(yaml_file_path, "r", encoding="utf-8") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    return config


def combine_pyspark_dfs(dfs: List[ps.DataFrame]) -> ps.DataFrame:
    """
    Combine multiple pyspark dataframes of same schema into one

    Parameters
    ----------
    dfs : List[ps.DataFrame]
        List of pyspark dataframes

    Returns
    -------
    ps.DataFrame
        Combined pyspark dataframe
    """

    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
