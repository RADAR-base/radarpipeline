#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(name='radarpipeline',
      version='0.0.1',
      description='A python feature generation and visualzation package use with RADAR project data.',
      url='https://github.com/RADAR-base/radar-pipeline',
      author='Heet Sankesara',
      author_email='heet.sankesara@kcl.ac.uk',
      packages=find_packages(),
      install_requires=[],
      test_suite='tests',
      include_package_data=True,
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Science/Research"
          ]
)