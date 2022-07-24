# radar-pipeline

A python feature generation and visualization package use with RADAR project data.

## How to run

**Note**: If you are using Windows, please install Spark and set environment variables as mentioned [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) before going through the installation below. Specifically, you'll need to have the environment variables set, as given [here](https://spark.apache.org/docs/1.6.0/configuration.html#environment-variables).

-   Clone the repository:

    ```bash
    $ git clone https://github.com/RADAR-base/radar-pipeline.git
    ```

-   Change the directory to `radar-pipeline`:

    ```bash
    $ cd radar-pipeline
    ```

-   Create a [virtual environment](https://virtualenv.pypa.io/en/latest/installation.html) and activate it.

    -   Install the virtualenv package:

        ```bash
        $ python -m pip install --user virtualenv
        ```

    -   Create a python virtual environment:

        ```bash
        $ python -m virtualenv env
        ```

    -   Activate the virtual environment:

        On Windows, run:

        ```bash
        $ .\env\Scripts\activate
        ```

        On Linux or MacOS, run:

        ```bash
        $ source ./env/bin/activate
        ```

-   Install the dependencies:

    ```bash
    $ pip install -r requirements.txt
    ```

-   Ensure that the in the `radrapipeline/radarpipelinne.py` file, the project is instantiated as `Project(input_data="mock-config.yaml")`.

-   Run the pipeline:

    ```bash
    $ python .
    ```

-   The pipeline would do a mock run and ingest the data in the `mock-data` directory.
