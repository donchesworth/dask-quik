#!/usr/bin/env python

from setuptools import setup, find_packages
from os import system
from pathlib import Path

here = Path(__file__).parent
README = here.joinpath("README.md").read_text()

with open(here.joinpath("requirements.txt")) as f:
    all_reqs = f.read().split("\n")

removes = ["git+"]
if system("nvidia-smi -L") != 0:
    removes.extend(["cudf", "cuda"])
install_requires = [
    x.strip() for x in all_reqs if not any(y in x for y in removes)
]


setup(
    name="dask-quik",
    version="0.0.3",
    description="function to make working in dask_cudf and dask quik-er",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/donchesworth/dask-quik",
    author="Don Chesworth",
    author_email="donald.chesworth@gmail.com",
    license="BSD",
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(exclude=["tests"]),
    install_requires=install_requires,
)
