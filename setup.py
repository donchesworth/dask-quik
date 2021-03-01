#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path, system

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt')) as f:
    all_reqs = f.read().split('\n')

removes = ['git+']
if (system("nvidia-smi -L") != 0):
    removes.extend(['cudf', 'cuda'])
install_requires = [x.strip() for x in all_reqs if not any(y in x for y in removes)]


setup(
    name='dask_quik',
    version='0.1',
    author="Don Chesworth",
    author_email="donald.chesworth@gmail.com",
    packages=find_packages(exclude=["test"]),
    install_requires=install_requires
)

