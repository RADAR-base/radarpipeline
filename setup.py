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
        "pyYaml==6.0",
        "pandas==1.4.1",
        "numpy==1.22.3",
        "scipy==1.10.0",
        "pyspark[sql]==3.5.0",
        "GitPython>=3.1.32",
        "strictyaml==1.7.3",
        "paramiko==3.1.0",
        "avro==1.11.2"],
    test_suite="tests",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Science/Research",
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
