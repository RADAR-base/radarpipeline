<h1 align="center">RADAR Pipeline</h1>

<p align="center">
<a href="https://github.com/RADAR-base/radar-pipeline/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/RADAR-base/radar-pipeline"></a>
<a href="https://github.com/RADAR-base/radar-pipeline/network"><img alt="GitHub forks" src="https://img.shields.io/github/forks/RADAR-base/radar-pipeline"></a>
<a href="https://github.com/RADAR-base/radar-pipeline/stargazers"><img alt="GitHub stars" src="https://img.shields.io/github/stars/RADAR-base/radar-pipeline"></a>
<a href="https://github.com/RADAR-base/radar-pipeline/blob/main/LICENSE"><img alt="GitHub license" src="https://img.shields.io/github/license/RADAR-base/radar-pipeline"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

<p align="center">An open-source python feature generation and visualization package use with RADAR project data.</p>

---

## Wiki

Please visit the [RADAR Pipeline Wiki](https://github.com/RADAR-base/radar-pipeline/wiki) to learn more about RADAR Pipeline.

Wiki resources:

-   [Home](https://github.com/RADAR-base/radar-pipeline/wiki)
-   [Installation](https://github.com/RADAR-base/radar-pipeline/wiki/Installation#installation-instructions)
-   [Contributor Guide](https://github.com/RADAR-base/radar-pipeline/wiki/Contributor-Guide)
-   [Mock Pipeline](https://github.com/RADAR-base/radar-pipeline/wiki/Mock-Pipeline)
-   [Configuration](https://github.com/RADAR-base/radar-pipeline/wiki/Configuration)

## How to run

> **Note**
> If you are using Windows, please install Spark and set environment variables as mentioned [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) before going through the installation below. You'll need to set the environment variables given [here](https://spark.apache.org/docs/1.6.0/configuration.html#environment-variables).

-   Clone the repository (with all the submodules):

    ```bash
    $ git clone --recurse-submodules https://github.com/RADAR-base/radar-pipeline.git
    ```

-   Change the directory to `radar-pipeline`:

    ```bash
    $ cd radar-pipeline
    ```

-   Checkout the development branch:

    ```bash
    $ git checkout dev
    ```

-   Create a [virtual environment](https://virtualenv.pypa.io/en/latest/installation.html) and activate it. The instructions here use `virtualenv` but feel free to use any python virtual environment manager of your choice.

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
    $ python -m pip install -r requirements.txt
    ```

-   To verify the installations, run the following command in the project root directory to run the pipeline:

    ```bash
    $ python .
    ```

-   The pipeline would do a mock run and ingest the data in the `mock-data` directory. You can see some outputs in the CLI and if the project is installed correctly, the mock pipeline would run without errors.

## License

This project is licensed under the [ Apache License, Version 2.0](https://github.com/RADAR-base/radar-pipeline/blob/main/LICENSE).
