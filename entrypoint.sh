#!/bin/sh

cd /opt/dq
pytest
# pytest --cov=. --cov-report=xml
# curl https://codecov.io/bash