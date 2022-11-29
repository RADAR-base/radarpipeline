import os
from functools import reduce
from typing import Any, Dict, List
from urllib.parse import urlparse
from pathlib import Path

import pyspark.sql as ps
import requests
import yaml
from strictyaml import load, Map, Str, Seq, Bool, Optional, YAMLError

from radarpipeline.common import constants


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
    schema = get_yaml_schema()
    with open(yaml_file_path, "r", encoding=constants.ENCODING) as file:
        config = load(file.read(), schema).data
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
    df_comb = reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs
    )
    return df_comb


def is_valid_github_path(path: str) -> bool:
    """
    Check if the current path is a valid github path

    Parameters
    ----------
    path : str
        Path to check
    Returns
    -------
    bool
        True if the path is valid, False otherwise
    """

    parsed = urlparse(path)
    if parsed.scheme not in ["https", "http"] or parsed.netloc != "github.com":
        return False

    if requests.head(path, allow_redirects=True).status_code != 200:
        return False

    return True


def get_repo_name_from_url(url: str) -> str:
    """
    Return the name of the repository from the url
    Reference: https://stackoverflow.com/a/55137835/10307491

    Parameters
    ----------
    url : str
        Url of the repository

    Returns
    -------
    str
        Name of the repository
    """
    last_slash_index = url.rfind("/")
    last_suffix_index = url.rfind(".git")
    if last_suffix_index < 0:
        last_suffix_index = len(url)

    if last_slash_index < 0 or last_suffix_index <= last_slash_index:
        raise Exception(f"Badly formatted url {url}")

    return url[last_slash_index + 1 : last_suffix_index]


def pascal_to_snake_case(s: str) -> str:
    """
    Convert a string from PascalCase to snake_case

    Parameters
    ----------
    s : str
        String to convert

    Returns
    -------
    str
        Converted string
    """
    return "".join(["_" + i.lower() if i.isupper() else i for i in s]).lstrip("_")


def get_yaml_schema() -> Map:
    schema = Map({
        "project": Map({
            "project_name": Str(),
            Optional("description"): Str(),
            Optional("version"): Str()
        }),
        "input_data": Map({
            "data_location": Str(),
            "local_directory": Seq(Str()) | Str(),
            "data_format": Str()
        }),
        "configurations": Map({
            "df_type": Str()
        }),
        "features": Seq(Map({
            "location": Str(),
            Optional("branch", default='main'): Str(),
            "feature_groups": Seq(Str())
        })),
        "output_data": Map({
            "output_location": Str(),
            "local_directory": Str(),
            "data_format": Str(),
            "compress": Bool()
        })
    })
    return schema
