import os
from typing import Any, Dict

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
