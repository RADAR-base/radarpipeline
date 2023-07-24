#!/usr/bin/env python3
from setuptools import find_packages, setup
import os


def read_file(filename):
    with open(os.path.join(os.path.dirname(__file__), filename)) as file:
        return file.read()


setup(
    name="radarpipeline",
    version="2.0.1a",
    license='Apache',
    description="A python feature generation and visualization package use with RADAR project data.",
    url="https://github.com/RADAR-base/radarpipeline",
    download_url='https://github.com/RADAR-base/radarpipeline/archive/refs/tags/v2.0.1a.tar.gz',
    readme="README.md",
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    author="Heet Sankesara",
    author_email="heet.sankesara@kcl.ac.uk",
    keywords=['mhealth', 'pipeline', 'big-data'],
    packages=find_packages(),
    install_requires=[],
    test_suite="tests",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Science/Research",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
