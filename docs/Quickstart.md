# Running the Mock Pipeline

The Mock Pipeline is a simple example of how to use the RADAR-pipeline to read data, compute features, and write the results. It is designed to help you get started with the RADAR-pipeline and understand its basic functionality. You can find the Mock Pipeline [here](https://github.com/RADAR-base/radarpipeline/wiki/Mock-Pipeline).

## Mock Pipeline Run

To run the Mock Pipeline, follow these steps:
1. **Install the RADAR-pipeline**: Follow the [installation instructions](index.md#installation) to install the RADAR-pipeline.

2. **Git clone Mock Features**:

   ```bash
    git clone https://github.com/RADAR-base-Analytics/mockfeatures.git
   ```

3. **Setting up the config file**:

```yaml
project:
    project_name: mock_project
    description: mock_description
    version: mock_version

input:
    source_type: mock # couldbe mock, local, sftp, s3
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

configurations:
    df_type: 'pandas'

features:
    - location: 'https://github.com/RADAR-base-Analytics/mockfeatures'
      branch: main
      feature_groups:
          - MockFeatureGroup
      feature_names:
        - all

output:
    output_location: local # can be local, postgres, sftp
    config:
        target_path: output/mockdata
    data_format: csv
    compress: false
```

4.1 **Run the Mock Pipeline using python** :

   ```bash
   python .
   ```
OR

4.2 **Run the Mock Pipeline using CLI**:
   ```bash
   radarpipeline run -f config.yaml
   ```

## Example Output
```bash
/Users/k2039563/miniconda3/envs/radarpipeline/lib/python3.10/site-packages/paramiko/pkey.py:100: CryptographyDeprecationWarning: TripleDES has been moved to cryptography.hazmat.decrepit.ciphers.algorithms.TripleDES and will be removed from cryptography.hazmat.primitives.ciphers.algorithms in 48.0.0.
  "cipher": algorithms.TripleDES,
/Users/k2039563/miniconda3/envs/radarpipeline/lib/python3.10/site-packages/paramiko/transport.py:259: CryptographyDeprecationWarning: TripleDES has been moved to cryptography.hazmat.decrepit.ciphers.algorithms.TripleDES and will be removed from cryptography.hazmat.primitives.ciphers.algorithms in 48.0.0.
  "class": algorithms.TripleDES,
2025-07-22 16:34:57,712 [INFO]: Starting the pipeline run...
2025-07-22 16:34:57,713 [INFO]: Reading and Validating the configuration file...
2025-07-22 16:34:57,721 [INFO]: Config file read successfully
2025-07-22 16:34:57,721 [INFO]: Mock data submodule not found. Cloning it...
2025-07-22 16:35:10,883 [INFO]: Mock data input cloned
2025-07-22 16:35:12,339 [INFO]: Using feature from cache location: /Users/k2039563/.cache/radarpipeline/mockfeatures
2025-07-22 16:35:12,339 [INFO]: Config file validated successfully
2025-07-22 16:35:12,342 [INFO]: Number of feature groups found: 1
2025-07-22 16:35:12,342 [INFO]: List of Feature groups: ['MockFeatureGroup']
2025-07-22 16:35:12,342 [INFO]: Total required data: {'android_phone_step_count', 'android_phone_battery_level'}
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/07/22 16:35:13 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting Spark log level to "OFF".
2025-07-22 16:35:14,286 [INFO]: Spark Session created
2025-07-22 16:35:14,286 [INFO]: Fetching the data...
2025-07-22 16:35:14,287 [INFO]: Reading data from old RADAR format
2025-07-22 16:35:14,287 [INFO]: Reading data for user: 2a02e53a-951e-4fd0-b47f-195a87096bd0
2025-07-22 16:35:14,288 [INFO]: Reading data for user: 07a69f47-1923-4cfc-b89b-0eefad483f43
2025-07-22 16:35:14,288 [INFO]: Reading data for user: 5c0e2ec7-6f85-4041-9669-7145075d1754
2025-07-22 16:35:14,288 [INFO]: Reading data for user: 072ddb22-82ef-4b81-8460-41ab096b54bb
2025-07-22 16:35:14,288 [INFO]: Reading data for variable: android_phone_step_count
2025-07-22 16:35:14,288 [INFO]: Reading data for variable: android_phone_step_count
2025-07-22 16:35:14,288 [INFO]: Reading data for variable: android_phone_step_count
2025-07-22 16:35:14,288 [INFO]: Reading data for variable: android_phone_step_count
2025-07-22 16:35:14,288 [INFO]: Schema found
2025-07-22 16:35:14,288 [INFO]: Schema found
2025-07-22 16:35:14,288 [INFO]: Schema found
2025-07-22 16:35:14,288 [INFO]: Schema found
2025-07-22 16:35:16,085 [INFO]: Reading data for variable: android_phone_battery_level
2025-07-22 16:35:16,085 [INFO]: Reading data for variable: android_phone_battery_level
2025-07-22 16:35:16,085 [INFO]: Reading data for variable: android_phone_battery_level
2025-07-22 16:35:16,085 [INFO]: Reading data for variable: android_phone_battery_level
2025-07-22 16:35:16,085 [INFO]: Schema found
2025-07-22 16:35:16,085 [INFO]: Schema found
2025-07-22 16:35:16,086 [INFO]: Schema found
2025-07-22 16:35:16,086 [INFO]: Schema found
2025-07-22 16:35:17,072 [INFO]: Closing down clientserver connection
2025-07-22 16:35:17,140 [INFO]: Closing down clientserver connection
2025-07-22 16:35:17,166 [INFO]: Closing down clientserver connection
2025-07-22 16:35:17,248 [INFO]: Closing down clientserver connection
2025-07-22 16:35:17,248 [INFO]: Computing the features...
2025-07-22 16:35:17,248 [INFO]: Computing feature PhoneBatteryChargingDuration
2025-07-22 16:35:17,576 [INFO]: Computing feature StepCountPerDay
2025-07-22 16:35:17,834 [INFO]: Exporting the features data...
2025-07-22 16:35:17,836 [INFO]: Feature PhoneBatteryChargingDuration data exported to output/mockdata/phone_battery_charging_duration.csv
2025-07-22 16:35:17,836 [INFO]: Feature StepCountPerDay data exported to output/mockdata/step_count_per_day.csv
2025-07-22 16:35:17,836 [INFO]: Data exported successfully. Closing Spark Engine
2025-07-22 16:35:17,956 [INFO]: Pipeline run completed successfully
2025-07-22 16:35:17,956 [INFO]: Closing down clientserver connection
```