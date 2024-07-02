#!/usr/bin/env python3
from setuptools import find_packages, setup
import os


def read_file(filename):
    with open(os.path.join(os.path.dirname(__file__), filename)) as file:
        return file.read()


setup(
    name="radarpipeline",
    version="2.1.0",
    license='Apache',
    description="A python feature generation and visualization package use with RADAR-base project data.",
    url="https://github.com/RADAR-base/radarpipeline",
    download_url='https://github.com/RADAR-base/radarpipeline/archive/refs/tags/v2.1.0.tar.gz',
    readme="README.md",
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    author="Heet Sankesara, Amos Folarin",
    author_email="heet.sankesara@kcl.ac.uk, amos.folarin@kcl.ac.uk",
    keywords=['mhealth', 'pipeline', 'big-data'],
    packages=find_packages(),
    install_requires=[
        "twine==5.1.0",
        "pyYaml==6.0.1",
        "pandas==2.2.2",
        "numpy==1.26.4",
        "scipy==1.14.0",
        "pyspark[sql]==3.5.1",
        "GitPython>=3.1.41",
        "strictyaml==1.7.3",
        "paramiko==3.4.0",
        "avro==1.11.3",
        "pyspark[sql]==3.5.1",
        "pyarrow==16.1.0"],
    test_suite="tests",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Science/Research",
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
