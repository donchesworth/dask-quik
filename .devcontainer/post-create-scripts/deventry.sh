#!/usr/bin/env bash

mkdir $IPYTHONDIR
ipython profile create
unalias cp
cp -r $DEVDIR/ipython_config.json $IPYTHONDIR/profile_default

pip install ceean_connectors[vault]
pip install --upgrade boto3==1.16.52 botocore==1.19.52 -y
pip uninstall psycopg2 -y
pip install --upgrade --force-reinstall psycopg2-binary==2.8.6
for dir in $BASEDIR/*; do
    pip install -e "$dir"
done
# pip install -e $BASEDIR/dask-quik/
# pip install -e $BASEDIR/solution-prediction-data/