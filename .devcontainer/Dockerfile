# Using my base rapids-dask container
# FROM ghcr.io/rapids-dask-ubi8:v2
FROM quay.io/donchesworth/rapids-dask-pytorch:py38-cuda10.2-rapids0.18-ubi8

# Labels
LABEL maintainer="Don Chesworth<donald.chesworth@gmail.com>"
LABEL org.label-schema.schema-version="0.1"
LABEL org.label-schema.name="dqsp_dev_container"
LABEL org.label-schema.description="a development container for dask-quik and solution-prediction"

ENV PATH="/opt/conda/envs/rd/bin:/opt/conda/bin:$PATH"
ENV IPYTHONDIR="/opt/rd/.ipythondir/"

SHELL ["/bin/bash", "-c"]
RUN source /opt/conda/bin/activate && conda activate rd && \
    conda install --name rd ipykernel -y

COPY requirements/ /tmp/
RUN pip install -r /tmp/r1.txt && pip install -r /tmp/r2.txt && \
    pip install flake8 black

RUN ipython profile create
