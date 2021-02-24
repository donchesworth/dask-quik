#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt')) as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]

setup(
    name='dask_quik',
    version='0.1',
    author="Don Chesworth",
    author_email="donald.chesworth@gmail.com",
    packages=find_packages(exclude=["test"]),
    install_requires=install_requires
)
