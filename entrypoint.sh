#!/bin/sh

echo $(pwd)
pytest
pytest --cov=/opt/dq/dask_quik/tests/ --cov-report=xml
curl https://codecov.io/bash