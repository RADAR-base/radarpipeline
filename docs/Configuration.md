# Configuration

The `config.yaml` file is the configuration file that would be used to configure different pipelines. 

The different components of the config file are **project**, **input_data**, **configurations**, **features** **output_data** and **spark_config**

## project

Project contains metadata for the pipeline such as the name and description of the pipeline.

* **project_name** - Name of the project. Must be a single string without any spaces.
* **description** - Description of the pipeline.
* **version** - Version of the pipeline

## input_data

* **data_type** -  Type of input data. It can be `local`, `mock`, `sftp` or `s3`. The `local` data type means the data is stored locally, the `mock` type means that the mock data have to be used. `sftp` implies that the data is stored in the sftp server. `s3` (Have not been implemented yet) would mean that the data is stored in s3 container. 

* **config** - For different data types, we use different config. 
    * *local* - For `local` data type, config only contains `source_path` which is the path to the local data file
    * *mock* - For `mock` data type, config contains `source_path` as `mockdata/mockdata`
    * *sftp* - For `sftp` data type, config needs `sftp_host` (location of the host), `sftp_source_path` (source path where the data is stored), `sftp_username` (sftp username), `sftp_password` (sftp password if applicable), `sftp_private_key` (sftp private key location) and `sftp_target_path` (Target path where sftp data should be stored). 

* **data_format** - Data format if the input files. We currently support `csv` and `csv.gz`. We are working on adding additional data formats in the near future.

## configurations

* **df_type** - Type of data frame the researchers are using. It currently supports `pandas` and `spark`

## features

A Feature is a variable that is implemented by the researchers and can be rerun by the user. A feature takes in an input, preprocesses it, does the computation, post-processes it, and finally returns the output.

Each pipeline should contain a collection of one or more features, all of which will be computed simultaneously. The collection of features are termed a `features_group` see [step-3-setup-the-repository](https://github.com/RADAR-base/radarpipeline/wiki/Setup#step-3-setup-the-repository)

* **location** - Location of the feature directory. This can be a GitHub repository or a local directory.

* **branch** - If the feature directory is a GitHub repository, you can specify the branch that you want to run. If there's no branch specified, it'll run the main branch.

* **feature_groups** - Name of the feature groups that will be computed during the pipeline run.

* **feature_names** - Name of the features from the corresponding feature groups that will be computed during the pipeline run.

## output_data

* **output_location** - target output options `postgres` (Not implemented yet) | `local`

* **config** - For different output locations, we use different config. 
    * *local* - For `local` data type, config only contains `target_path` which is the path of the directory where data should be saved. 

* **data_format** - Format of the output data. Currently, only supports `csv` is supported.

* **compress** - Boolean configuration to compress the data

## spark_config

Customizable spark configuration with default values. The spark config with default values will work fine most of the time but if any of the parameters needed to be changed, it can be changed from here. The parameters and their default values are as follows. 
```yaml
spark.executor.instances: 4
spark.executor.cores: 4
spark.executor.memory: 10g
spark.driver.memory: 15g
spark.memory.offHeap.enabled: True
spark.memory.offHeap.size: 20g
spark.driver.maxResultSize: 0
```


# Mock Configuration file

```yaml

# 'project' defines the metadata of the project
project:
    project_name: mock_project
    description: mock_description
    version: mock_version

# 'input_data' defines how the data would be ingested by the pipeline
input:
    data_type: mock # couldbe mock, local, sftp, s3
    config:
        # In case of sftp, use the following format
        # sftp_host:
        # sftp_source_path:
        # sftp_username:
        # sftp_private_key:
        # sftp_target_path:
        # In case of s3, use the following format
        #       aws_access_key_id:
        #       aws_secret_access_key:
        #       region_name:
        #       s3_access_url:
        #       bucket:
        #       prefix:
        # In case of local or Mock, use the following format
        source_path: mockdata/mockdata
    data_format: csv

# configuration including target data structure
configurations:
    df_type: 'pandas'

# 'features' define what features to use for data processing
features:
    - location: 'https://github.com/RADAR-base-Analytics/mockfeatures'
      branch: main
      feature_groups:
          - MockFeatureGroup
      feature_names:
        - all

# 'input_data' defines how the data would be exported by the pipeline
output:
    output_location: local # can be local or postgres
    config:
        target_path: output/mockdata
    data_format: csv
    compress: false

# 'spark_config' customizes spark configuration
spark_config:
    spark.executor.instances: 4
    spark.executor.cores: 4
    spark.executor.memory: 10g
    spark.driver.memory: 15g
    spark.memory.offHeap.enabled: True
    spark.memory.offHeap.size: 20g
    spark.driver.maxResultSize: 0
```
