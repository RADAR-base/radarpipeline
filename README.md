# radar-pipeline

A python feature generation and visualization package use with RADAR project data.

## How to run

**Note**: If you are using Windows, please install Spark and set environment variables as mentioned [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) before going through the installation below. Specifically, you'll need to have the environment variables set, as given [here](https://spark.apache.org/docs/1.6.0/configuration.html#environment-variables).

-   Clone the repository (with all the submodules):

    ```bash
    $ git clone --recurse-submodules https://github.com/RADAR-base/radar-pipeline.git
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

-   Run the following command in the project root directory to run the pipeline:

    ```bash
    $ python .
    ```

-   The pipeline would do a mock run and ingest the data in the `mock-data` directory.

## A note on submodules

The project uses git submodules to fetch additional resources such as the mock data. While cloning the repo for the first time, they are downloaded with the repo, if the `--recurse-submodules` flag is provided to the `git clone` command.

If the submodule needs to be updated again, run the following command in the project root directory:

```bash
$ git submodule update --init --recursive
```

## For contributors

This project uses [_pre-commit_](https://pre-commit.com/) to run [_isort_](https://pycqa.github.io/isort/), [_flake8_](https://flake8.pycqa.org/en/latest/), and [_black_](https://black.readthedocs.io/en/stable/) on the codebase before each commit.

To initialize the pre-commit hooks, run the following command in the project root directory:

```bash
$ pre-commit install
```
