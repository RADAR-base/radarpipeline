# Setting up a new pipeline

The [mock feature pipeline](https://github.com/RADAR-base-Analytics/mockfeatures) is the test pipeline that we have created to provide user an intro to how to run radar pipeline. It is highly recommended that you run it first.

This is a guide to how to create and configure a new pipeline. We'll use the [mock feature pipeline](https://github.com/RADAR-base-Analytics/mockfeatures) as an example.

### Step 1. Create a new github repository

Create a new repository on GitHub. the naming convention for the new repository is to not use any special characters like hypher or underscore. For example, the Mock Feature pipeline us called `mockfeatures`. It is adviced to use the same naming convention.

### Step 2. Install radar-pipeline module

To install the radar-pipeline module, follow the instructions in the How to run section in [README.md](../README.md)

### Step 3. Setup the repository


Checkout the [Mock feature pipeline](https://github.com/RADAR-base-Analytics/mockfeatures) to see the structure of each pipeline.

1. Create `__init__.py` and another directory in the repo with the same name as the repository.

2. This is the directory where all the pipeline code should be located. It is not a requirement but it can make it much more easier for users to locate all the code. In the directory create `features.py`.
__Note__ :- You can create python file with any other name or any number of python files.

3. In the python file, create feature groups using the class FeatureGroup. As mentioned earlier, each feaure feature group would contain a collection of features.
    The mock feature group looks like this:

    ```python
    from radarpipeline.datalib import RadarData
    from radarpipeline.features import Feature, FeatureGroup

    class MockFeatureGroup(FeatureGroup):
        def __init__(self):
            name = "MockFeatureGroup"
            description = "contains mock features"
            features = [PhoneBatteryChargiångDuration, StepCountPerDay]
            super().__init__(name, description, features)

        def preprocess(self, data: RadarData) -> RadarData:
            """
            Preprocess the data for each feature in the group.
            """
            return data
    ```

    The `name` and `description` would give name and description of the feature group respectively.

    the `features` variables is the list of feature classes that are part of this feature group. We''' explain in next step how to define a feature class.

    The `preprocess` funtion is supposed to run all the preprocessing steps common in all the features. This is a generic preprocessing steps which should be used to do any generic preprocessing or handing of missing data.

4. Now define the feature class using `Feature` as the base class. In the mock data pipeline, we have defined two features `PhoneBatteryChargingDuration` and `StepCountPerDay`
å
    ```py
    class PhoneBatteryChargingDuration(Feature):
        def __init__(self):
            self.name = "PhoneBatteryChargingDuration"
            self.description = "The duration of the phone battery charging"
            self.required_input_data = ["android_phone_battery_level"]

        def preprocess(self, data: RadarData) -> RadarData:
            """
            Preprocess the data for each feature in the group.
            """
            df_phone_battery_level = data.get_combined_data_by_variable(
                "android_phone_battery_level"
            )
            df_phone_battery_level["time"] = pd.to_datetime(
                df_phone_battery_level["value.time"], unit="s"
            )
            df_phone_battery_level["date"] = df_phone_battery_level["time"].dt.date
            df_phone_battery_level = df_phone_battery_level[
                ~df_phone_battery_level[
                    ["key.userId", "value.time", "value.batteryLevel"]
                ].duplicated()
            ]
            return df_phone_battery_level

        def calculate(self, data) -> float:
            df_phone_battery_level = data
            df_phone_battery_level["value.statusTime"] = (
                df_phone_battery_level.groupby("key.userId")["value.time"].diff().shift(-1)
            )
            df_phone_battery_level = (
                df_phone_battery_level.groupby(["key.userId", "date", "value.status"])
                .agg({"value.statusTime": "sum"})
                .reset_index()
            )

            df_phone_battery_level = df_phone_battery_level[
                df_phone_battery_level["value.status"] == "CHARGING"
            ]
            df_phone_battery_level["value.statusTimeInSeconds"] = (
                df_phone_battery_level["value.statusTime"].dt.total_seconds() / 60
            )
            return df_phone_battery_level


    class StepCountPerDay(Feature):
        def __init__(self):
            self.name = "StepCountPerDay"
            self.description = "The number of steps per day"
            self.required_input_data = ["android_phone_step_count"]

        def preprocess(self, data: RadarData) -> RadarData:
            """
            Preprocess the data for each feature in the group.
            """
            df_step_count = data.get_combined_data_by_variable("android_phone_step_count")
            df_step_count["time"] = pd.to_datetime(df_step_count["value.time"], unit="s")
            df_step_count["date"] = df_step_count["time"].dt.date
            df_step_count = df_step_count[
                ~df_step_count[["key.userId", "value.time", "value.steps"]].duplicated()
            ]
            df_step_count = df_step_count.reset_index(drop=True)
            return df_step_count

        def calculate(self, data) -> float:
            df_step_count = data
            df_total_step_count = df_step_count.groupby(["key.userId", "date"]).agg(
                {"value.steps": "sum"}
            )
            df_total_step_count = df_total_step_count.reset_index()
            return df_total_step_count
    ```

    In each of the `Feature` class, there's `__init__`, `preprocess` and `calculate` function.

    In the `__init__` function, the name and the description of the function has to be specified. It is also required to mention variables needed to compute the feature in `required_input_data`. All the data reading will be done by the underlying pyspark module.

    In the `preprocess` function, all the preprocessing steps required to compute the feature has to be written. If there's no preprocessing step, return the input `data`.

    In the `calculate` function, the code of computing the feature has to be written.

5. After all the features and feature groups have been written, in the `__init__.py`, it is needed to import all the feature groups and features.

    ```py
    from .mockfeatures.features import (
        MockFeatureGroup,
        PhoneBatteryChargingDuration,
        StepCountPerDay,
    )
    ```

Now the feature pipeline is finished. The last step is to create a config file and test the module.

### Step 4. Create a config file

Please follow the format of the config file for the feature pipeline written in [config.md]("./config.md)

The mock configuration file looks like this:

```yaml
project:
    project_name: mock_project
    description: mock_description
    version: mock_version

input_data:
    data_location: mock
    local_directory: mockdata/mockdata
    data_format: csv

configurations:
    df_type: 'pandas'

features:
    - location: 'https://github.com/RADAR-base-Analytics/mockfeatures'
      feature_groups:
          - MockFeatureGroup

output_data:
    output_location: local
    local_directory: output/mockdata
    data_format: csv
    compress: false
```