# Core Concepts

This document describes the core concepts of RADAR-pipeline. It is intended to help you understand how RADAR-pipeline works and how to use it effectively.

## YAML Configuration

RADAR-pipeline uses YAML files for configuration. These files define the structure and behavior of the pipeline, including the steps to be executed, their parameters, and how they interact with each other.

### Mock YAML Configuration

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

For in-detail explanations about the configuration options, refer to the [Configuration Reference](https://github.com/RADAR-base/radarpipeline/wiki/Configuration).

## Features and Feature Groups

Features are the building blocks of RADAR-pipeline. They define the data processing logic and can be grouped into feature groups for better organization. To create a new feature, you need to implement a class that inherits from the `Feature` base class and define the `preprocess` and `compute` methods. Radar-pipeline will automatically discover and register these features when the pipeline is executed and handles the data reading, processing, and writing.

Feature groups allow you to organize related features together, making it easier to manage and execute them as a unit. You can define a feature group by creating a class that inherits from the `FeatureGroup` base class and specifying the features it contains. In Feature Groups, you can also define `preprocess` method to handle the data pre-processing logic for the entire group. 

For more information on how to create features and feature groups, refer to the [Feature Development Guide](https://github.com/RADAR-base/radarpipeline/wiki/Setup)

## Pipeline Execution

Radar-pipeline uses Spark in the backend to read and process large datasets efficiently. It leverages Spark's distributed computing capabilities to scale out the data processing tasks across multiple nodes, ensuring high performance and reliability. The pipeline execution is designed to be fault-tolerant, allowing it to recover from failures and continue processing without losing data. The pipeline utilises AVRO schema associated with the data to ensure that the data is processed correctly, efficiently and consistently across different stages of the pipeline.

## Data Ingestion
Data ingestion is the process of bringing data into the RADAR-pipeline for processing. The pipeline supports various data sources, including local files and SFTP servers. We currently support the radar-base data format but planning to add future support for custom data formats. The configuration file specifies the source of the data and its format, allowing the pipeline to read and process it accordingly.


## Data Tabularisation

Data tabularisation is the process of transforming raw data into a structured format that can be easily analyzed and processed. This involves organizing the data into tables with rows and columns, where each column represents a specific feature or attribute of the data, and each row represents a single observation or record. We have implemented a tabularisation feature that automatically converts the raw data into a tabular format, making it easier to work with and analyze. The tabularisation process is customizable through the configuration file, allowing users to specify how the data should be structured and what features should be included.

### Tabularisation Configuration

```yaml
features:
    # Custom features
    - location: custom
      feature_groups:
        - Tabularize
      feature_names:
        - android_phone_battery_level, android_phone_step_count
```

The above configuration specifies that the `Tabularize` feature group should be used to tabularize the data, and it includes two features: `android_phone_battery_level` and `android_phone_step_count`. The pipeline will automatically apply the tabularization logic defined in the `Tabularize` feature group to these features, transforming the raw data into a structured format. It can then be exported or further processed as needed.

## Data Sampling

```yaml
configurations:
    user_sampling:
        ## Possible methods: fraction, count, userid
        method: fraction
        config:
            fraction: 0.8
        method: count
        config:
            count: 2
        method: userid
        config:
            userids:
                - 2a02e53a-951e-4fd0-b47f-195a87096bd0
    data_sampling:
        ## Possible methods:  time, count, fraction
        ## starttime and endtime format is dd-mm-yyyy hh:mm:ss in UTC timezone
        ## It is possible to have multiple time ranges. See below Example
        #method: time
        config:
        - starttime: 2018-11-22 00:00:00
          endtime:  2018-11-26 00:00:00
          time_column: value.time
        - starttime: 2018-12-27 00:00:00
          time_column: value.time
        method: count
        config:
           count: 100
        method: fraction
        config:
           fraction: 0.3
```

Data sampling is the process of selecting a subset of data from a larger dataset for analysis or processing. This is useful when working with large datasets, as it allows you to work with a smaller, more manageable portion of the data while still retaining its overall characteristics. The pipeline supports various sampling methods, including random sampling, stratified sampling, and systematic sampling. The configuration file allows you to specify the sampling method and parameters, such as the fraction of data to sample or the number of samples to select.

## Data Export

```yaml
output:
    output_location: local
    config:
        target_path: output/mockdata/
    data_format: csv
    compress: false
```

Data export is the process of saving the processed data to a specified location for further analysis or use. The pipeline supports various output formats, including CSV, Parquet, and AVRO. The configuration file specifies the target location and format for the exported data, allowing you to easily save the results of your data processing tasks. You can also specify whether to compress the exported data to save storage space. We currently support local as output locations, with plans to add support for other storage systems in the future.

## Publish Citable Pipelines

A Zenodo community page for analysis pipelines can be found [here](https://zenodo.org/communities/radar-base-analytics/?page=1&size=20) we will aim to curate this community page with repositories added to the [RADAR-base Analytics catalogue](https://github.com/RADAR-base-Analytics).

You may add citations to the RADAR-base pipeline using either or both the GitHub CITATIONS.cff file and the [Zenodo](https://zenodo.org/) badge.
 - [GitHub CFF File](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-citation-files )
 - [CFF website](https://citation-file-format.github.io/)
 - [CFF schema](https://github.com/citation-file-format/citation-file-format/blob/main/schema-guide.md)

You can add a CITATION.cff file to the root of a repository to let others know how you would like them to cite your work. The citation file format is plain text with human- and machine-readable citation information.

Example CITATION.cff file:

```cff
cff-version: 1.2.0
message: "If you use this software, please cite it using listed below."
authors:
- family-names: "Sankesara"
  given-names: "Heet"
  orcid: "https://orcid.org/0000-0002-9126-5615"
- family-names: "Patel"
  given-names: "Pushkar"
  orcid: "https://orcid.org/0000-0003-3410-8856"
- family-names: "Folarin"
  given-names: "Amos"
  orcid: "https://orcid.org/0000-0002-0333-1927"
title: "Mock Features"
version: 1.0.0
doi: 10.5281/zenodo.7248672
date-released: 2022-10-25
url: "https://github.com/RADAR-base-Analytics/mockfeatures"
```

Citable Zenodo DOI URLs will automatically get created for any analytics repository in this Github Organisation whenever you create a new release. When you create a new analytics repo, add a `CITATION.cff` file then create a pull request and tag `@afolarin` and `@Hsankesara` to review (and tag in the PR description `@RADAR-base-Analytics/pipeline-catalog-admins`) and publish it on the Zenodo community. Next, create a release, this will then be picked up by Zenodo and a DOI will be created (e.g. 10.5281/zenodo.7248672), we will then add the DOI URL to the CITATION.tff). Lastly, add any relevant topics to the repository e.g. `data`, `feature-extraction` etc.