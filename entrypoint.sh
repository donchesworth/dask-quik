#!/bin/sh

pytest --cov=./dask_quik/tests/ --cov-report=xml
curl https://codecov.io/bash