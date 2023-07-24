#!/usr/bin/env python3
from setuptools import find_packages, setup

setup(
    name="radarpipeline",
    version="2.0.1",
    license='Apache',
    description="A python feature generation and visualization package use with RADAR project data.",
    url="https://github.com/RADAR-base/radarpipeline",
    author="Heet Sankesara",
    author_email="heet.sankesara@kcl.ac.uk",
    download_url='https://github.com/RADAR-base/radarpipeline/archive/refs/tags/v2.0.1.tar.gz',
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
