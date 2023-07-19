<h1 align="center">RADAR Pipeline</h1>

<p align="center">
<a href="https://github.com/RADAR-base/radarpipeline"><img alt="GitHub branch checks state" src="https://img.shields.io/github/checks-status/RADAR-base/radarpipeline/main"></a>
<a href="https://github.com/RADAR-base/radarpipeline/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/RADAR-base/radarpipeline"></a>
<a href="https://github.com/thepushkarp/radarpipeline/pulls"><img alt="GitHub pull requests" src="https://img.shields.io/github/issues-pr/radar-base/radarpipeline"></a>
<a href="https://github.com/RADAR-base/radarpipeline/network"><img alt="GitHub forks" src="https://img.shields.io/github/forks/RADAR-base/radarpipeline"></a>
<a href="https://github.com/RADAR-base/radarpipeline/stargazers"><img alt="GitHub stars" src="https://img.shields.io/github/stars/RADAR-base/radarpipeline"></a>
<a href="https://github.com/RADAR-base/radarpipeline/blob/main/LICENSE"><img alt="GitHub license" src="https://img.shields.io/github/license/RADAR-base/radarpipeline"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

<p align="center">An open-source python feature generation and visualization package use with RADAR project data.</p>

---

## Wiki

Please visit the [RADAR Pipeline Wiki](https://github.com/RADAR-base/radarpipeline/wiki) to learn more about RADAR Pipeline. Also see the [RADAR-base Analytics Catalogue](https://github.com/RADAR-base-Analytics) for available pipelines for processing RADAR-base data.

Wiki resources:

-   [Home](https://github.com/RADAR-base/radarpipeline/wiki)
-   [Installation](https://github.com/RADAR-base/radarpipeline/wiki/Installation#installation-instructions)
-   [Contributor Guide](https://github.com/RADAR-base/radarpipeline/wiki/Contributor-Guide)
-   [Mock Pipeline](https://github.com/RADAR-base/radarpipeline/wiki/Mock-Pipeline)
-   [Configuration](https://github.com/RADAR-base/radarpipeline/wiki/Configuration)
-   [Data Ingestion](https://github.com/RADAR-base/radarpipeline/wiki/Data-Ingestion)
-  [Setup](https://github.com/RADAR-base/radarpipeline/wiki/Setup)
-  [Pipeline Core Topics](https://github.com/RADAR-base/radarpipeline/wiki/Pipeline-Core-Topics)
- [Creating Citable Analytics Pipelines](https://github.com/RADAR-base/radarpipeline/wiki/Creating-Citable-Analytics-Pipelines)

## How to run

> **Note**
>
> If you are using Windows, please install Spark and set environment variables as mentioned [here](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/) before going through the installation below. You'll need to set the environment variables given [here](https://spark.apache.org/docs/1.6.0/configuration.html#environment-variables).

-   Clone the repository (with all the submodules):

    ```bash
    $ git clone --recurse-submodules https://github.com/RADAR-base/radarpipeline.git
    ```

-   Change the directory to `radarpipeline`:

    ```bash
    $ cd radarpipeline
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

-   Install the module as a python package by running the command

    ```bash
    $ python -m pip install -e .
    ```

-   To verify the installations, run the following command in the project root directory to run the pipeline:

    ```bash
    $ python .
    ```

-   The pipeline would do a mock run and ingest the data in the `mock-data` directory. You can see some outputs in the CLI and if the project is installed correctly, the mock pipeline would run without errors and save the data to the `output` directory.

## License

This project is licensed under the [ Apache License, Version 2.0](https://github.com/RADAR-base/radarpipeline/blob/main/LICENSE).

## Citation & Acknowledgment 
Please use citation [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.7249526.svg)](https://doi.org/10.5281/zenodo.7249526) or or see [CITATION.cff](/CITATION.cff)

[Pushkar patel](https://github.com/thepushkarp) has done a great amount of work under Google Summer of Code 2022. His work report can be found [here](https://thepushkarp.github.io/RADAR-Base-GSoC-2022-Report/). We would like to thank Pushkar for all his contribution and GSoC for giving us this opportunity. 
