# Running the Radar Pipeline in Jupyter Notebooks or as a python library

To provide a more interactive experience for users, you can run the RADAR-pipeline in Jupyter notebooks. This allows you to visualize the data processing steps and results in real-time.

To get started, you need to install the necessary dependencies and set up your Jupyter notebook environment.

### Step 1. Install Jupyter Notebook and RADAR-pipeline

You can install Jupyter Notebook and the RADAR-pipeline module using pip. Open your terminal and run the following commands:

```bash
pip install jupyter
pip install radarpipeline
```

### Step 2. Create a Jupyter Notebook

Create a new Jupyter notebook file (e.g., `radar_pipeline_notebook.ipynb`) and open it in Jupyter Notebook.

### Step 3. Import RADAR-pipeline in the Notebook

In your Jupyter notebook, you can import the RADAR-pipeline module and start using it. Here is an example of how to set up a basic pipeline:

```python
from radarpipeline import radarpipeline
```

`radarpipeline` module have several handy functions to help you set up, read the data and run any pipeline. 

To read and process data, you can use the following code snippet:

```python
variables = ['android_app_battery_level', 'android_app_step_count']
datas = radarpipeline.read(PATH, variables)
```
This code reads the specified variables from the data source and prepares them for processing.

To get the android_app_battery_level and android_app_step_count features, you can use the following code:

```python
android_app_battery_level = datas.get_combined_data_by_variable('android_app_battery_level')
android_app_step_count = datas.get_combined_data_by_variable('android_app_step_count')
```

The data is now ready as a pandas DataFrame, and you can perform further analysis or visualization using standard pandas operations.

### Step 4. Running a pipeline

You can use `compute_features` method to configure the pipeline and compute features. Here is an example configuration:


```python
input_config={
            "source_type": "local",
            "config": {
                "source_path": PATH
            },
            "data_format": "csv"
        }
feature_config={
            "location": "https://github.com/RADAR-base-Analytics/appdatacategorization",
            "feature_groups": ["AppCategorizationFeatures"],
            "feature_names": [["CategorizeApp"]]
        }
data = radarpipeline.compute_features(input_config, feature_config)
df = data['CategorizeApp']
```

This example can compute the app categorization features from the data source specified in `input_config` using the feature configuration provided in `feature_config`.

### Show available pipelines

To see the available pipelines, you can use the following code snippet:

```python
from radarpipeline import radarpipeline
print(radarpipeline.get_available_pipelines())
```

The output will list all the pipelines that are available in the RADAR-pipeline module, allowing you to choose which one to run.

You can use the same function and methods in any Python script or application, making RADAR-pipeline a versatile tool for data processing and feature extraction in various environments.