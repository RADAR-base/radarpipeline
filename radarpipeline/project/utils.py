import yaml

def read_yaml(yaml_file_path: str):
    with open(yaml_file_path) as file:
        config = yaml.full_load(file)
    return config