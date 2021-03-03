#!/bin/sh

pytest --cov=./dask-quik/tests/ --cov-report=xml
curl https://codecov.io/bash