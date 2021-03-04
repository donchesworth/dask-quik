#!/bin/sh

pytest --cov=/opt/dq/dask_quik/tests/ --cov-report=xml
curl https://codecov.io/bash