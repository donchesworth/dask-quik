#!/bin/sh

echo $(pwd)
pytest --cov=. --cov-report=xml
# curl https://codecov.io/bash