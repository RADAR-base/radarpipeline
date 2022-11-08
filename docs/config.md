# Configuration

The `config.yaml` file is the configuration file that would be used to configure different pipelines. The configuration

The different components of the config file are **project**, **input_data**, **configurations**, **features** and **output_data**

## project

Project contains metadata for the pipeline such as name and description of the pipeline.

* **project_name** - Name of the project. Must be a single string without any spaces.
* **description** - Description of the pipeline.
* **version** - Version of the pipeline

## input_data

* **data_location** -  Location of the input data. It can be `local` or `mock`. The `local` data location means the data is stored locally while the `mock` location means that the mock data have to be used.

* **local_directory** -  When `data_location` is `local`, the path to the local data directory needs to be provided here. If the `data_location` is `mock`, then the `mockdata/mockdata` should be provided here. Otherwise

* **data_format** - Data format if the input files. We currently support `csv` and `csv.gz`. We are working on adding addition data formats as well.

## configurations

* **df_type** - Type of data frame the researchers are using. It currently supports `pandas` and `spark`

## features

A Feature is a variable that is implemented by the researchers and can be rerun by user. A feature takes in an input, preprocesses it, does the computation, post-processes it, and finally returns the output.

Each pipeline contains a collection of features all of which can be computed simultaneously. Those collections are named as `features_groups`

* **location** - Location of the feature directory. it can be a github repository or a local directory.

* **branch** - If the feature directory is a github repository, you can specify the branch that you want to run. If there's no branch specified, it'll run the main branch.

* **feature_groups** - Name of the feature groups that will be computed during the pipeline run.

## output_data

* **output_location** - target output options `mock` | `local`

* **local_directory** - Directory where the output data should be written

* **data_format** - Format of the output data. It currently only supports `csv`.

* **compress** - Boolean configuration to compress the data

# Mock Configuration file

```yaml

# 'project' defines the metadata of the project
project:
    project_name: mock_project
    description: mock_description
    version: mock_version

# 'input_data' defines how the data would be ingested by the pipeline
input_data:
    data_location: mock # location of the data (local path or 'mock')
    local_directory: mockdata/mockdata # path to data directory
    data_format: csv # format of the data ('csv')

# configuration including target data structure
configurations:
    df_type: 'pandas'

# 'features' define what features to use for data processing
features:
    - location: 'https://github.com/RADAR-base-Analytics/mockfeatures'
      feature_groups:
          - MockFeatureGroup

# 'input_data' defines how the data would be exported by the pipeline
output_data:
    output_location: local  # target output options `mock` | `local`)
    local_directory: output/mockdata
    data_format: csv
    compress: false

```