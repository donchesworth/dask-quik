#!/bin/sh

cd /opt/dq
# pytest
pytest --cov-config=.coveragerc
# curl https://codecov.io/bash