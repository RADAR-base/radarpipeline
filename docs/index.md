# RADAR Pipeline

![GitHub branch checks state](https://img.shields.io/github/checks-status/RADAR-base/radarpipeline/main)
![GitHub issues](https://img.shields.io/github/issues/RADAR-base/radarpipeline)
![GitHub pull requests](https://img.shields.io/github/issues-pr/radar-base/radarpipeline)
![GitHub forks](https://img.shields.io/github/forks/RADAR-base/radarpipeline)
![GitHub stars](https://img.shields.io/github/stars/RADAR-base/radarpipeline)
![GitHub license](https://img.shields.io/github/license/RADAR-base/radarpipeline)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)

An open-source python feature generation and visualization package use with RADAR project data.

---

Radar-pipeline is a Python package that provides a feature-based architecture for building data pipelines. It allows you to easily ingest, process, and export data while leveraging existing features and adding custom functionality.

## Installation

**NOTE**: You need to download JAVA(JDK) first on your desktop. 

### Installation using PIP

To install RADAR-pipeline, you can use the following command:

```bash
pip install radarpipeline
```

### Installation in a Conda environment

To install RADAR-pipeline using Conda, you can use the following command:

```bash
conda create -n radarpipeline python=3.10
conda activate radarpipeline
pip install radarpipeline
```

### Installation from source

To install RADAR-pipeline from source, follow the steps below. This is the recommended way to install RADAR-pipeline if you want to contribute to the project or if you want to use the latest features that are not yet released on PyPI.

!!! note
    If you are using Windows, please install Spark and set environment variables as mentioned [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) before going through the installation below. You'll need to set the environment variables given [here](https://spark.apache.org/docs/1.6.0/configuration.html#environment-variables).

Clone the repository (with all the submodules):

```bash
git clone --recurse-submodules https://github.com/RADAR-base/radarpipeline.git
```

Change the directory to `radarpipeline`:

```bash
cd radarpipeline
```

Checkout the development branch:

```bash
git checkout dev
```

Create a [Conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) and activate it.

```bash
conda create -n radarpipeline python=3.10
conda activate radarpipeline
```

Install the dependencies:

```bash
python -m pip install -r requirements.txt
```

Install the module as a python package by running the command:

```bash
python -m pip install -e .
```

To verify the installations, run the following command in the project root directory to run the pipeline:

```bash
python .
```

The pipeline would do a mock run and ingest the data in the `mock-data` directory. You can see some outputs in the CLI and if the project is installed correctly, the mock pipeline would run without errors and save the data to the `output` directory.

## Documentation

- [Introduction](Introduction.md)
- [Core Concepts](Core Concepts.md)
- [Quickstart](Quickstart.md)
- [Create your first pipeline](Create your first pipeline.md)
- [RADAR Pipeline & Jupyter Notebooks](Radarpipeline & jupyter notebooks.md)
- [RADAR Pipeline as a CLI tool](Radarpipeline as a CLI tool.md)
- [Radarpipeline for Big Data](Radarpipeline for Big data.md)
- [Why RADAR Pipeline?](Why Radarpipeline.md)

## Radar pipeline as a library

RADAR-pipeline can be used as a library in a python script or a jupyter notebook. You can use the `radarpipeline` module to run the pipeline, validate the configuration file, read the radar data locally, download the data from Radar-base sftp server, convert the data to another format such as parquet, compute any features from a featurepipeline and get the output in return in pandas, and list all the available feature pipelines.

To run a feature pipeline using the `config.yaml` file, you can use the following command:

```python
import radarpipeline
radarpipeline.run(config_file="config.yaml", variables)
```

To validate the configuration file, you can use the following command:

```python
import radarpipeline
radarpipeline.validate(config_file="config.yaml")
```

To read the radar data locally, you can use the following command:

```python
import radarpipeline
radarpipeline.read(source_path, )
```

To download the data from the sftp server, you can use the following command:

```python
import radarpipeline
input_config = {
    "input": {
        "source_type": "sftp",
        "config": {
            "sftp_host": "",
            "sftp_source_path": "",
            "sftp_username": "",
            "sftp_private_key": "",
            "sftp_target_path": "/path/to/data",
        },
        "data_format": csv
        }
    }
radarpipeline.fetch(input_config)
```

To convert the data to another format such as parquet, you can use the following command:

```python
import radarpipeline
data_format='parquet'
radarpipeline.convert(source_path, destination_path, variables, data_format)
```

To compute any features from a featurepipeline and get the output in return in pandas, you can use the following command:

```python
import radarpipeline
input_config={
            "source_type": "local",
            "config": {
                "source_path": "mockdata/mockdata"
            },
            "data_format": "csv"
        }
feature_config={
            "location": "custom",
            "feature_groups": ["Tabularize"],
            "feature_names": [["android_phone_battery_level"]]
        }
data = radarpipeline.compute_features(input_config, feature_config)
```

To list all the available feature pipelines, you can use the following command:

```python
import radarpipeline
print(radarpipeline.show_available_pipelines())
```

## Radar pipeline as a command line tool

RADAR-pipeline can be used as a command line tool. You can use the `radarpipeline` command to run the pipeline, validate the configuration file, read the radar data locally, download the data from Radar-base sftp server, convert the data to another format such as parquet, compute any features from a featurepipeline and get the output in return in pandas, and list all the available feature pipelines.

To list all the available commands, you can use the following command:

```bash
radarpipeline -h
```

Output:

```
A CLI interface for radarpipeline

positional arguments:
  {run,validate,generate,fetch,convert,list}
                        Sub-command help
    run                 Runs radarpipeline
    validate            Validate config file to run radarpipeline
    generate            Generates a mock config file to run radarpipeline
    fetch               Fetch data using config file
    convert             Convert radar data to custom format
    list                List available Pipelines

options:
  -h, --help            show this help message and exit
```

To run a feature pipeline using the `config.yaml` file, you can use the following command:

```bash
radarpipeline run --config config.yaml
```

## License

This project is licensed under the [Apache License, Version 2.0](https://github.com/RADAR-base/radarpipeline/blob/main/LICENSE).

## Citation & Acknowledgment

Please use citation [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7249526.svg)](https://doi.org/10.5281/zenodo.7249526) or or see [CITATION.cff](https://github.com/RADAR-base/radarpipeline/blob/main/CITATION.cff)

[Pushkar patel](https://github.com/thepushkarp) has done a great amount of work under Google Summer of Code 2022. His work report can be found [here](https://thepushkarp.github.io/RADAR-Base-GSoC-2022-Report/). We would like to thank Pushkar for all his contribution and GSoC for giving us this opportunity.

## Wiki

Please visit the [RADAR Pipeline Wiki](https://github.com/RADAR-base/radarpipeline/wiki) to learn more about RADAR Pipeline. Also see the [RADAR-base Analytics Catalogue](https://github.com/RADAR-base-Analytics) for available pipelines for processing RADAR-base data.
