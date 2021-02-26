ARG CUDA_VERSION=10.2
ARG RAPIDS_VERSION=0.17
FROM nvidia/cuda:${CUDA_VERSION}-base-centos7

# Labels
LABEL maintainer="Don Chesworth<donald.chesworth@gmail.com>"
LABEL org.label-schema.schema-version="0.1"
LABEL org.label-schema.name="dask-quik"
LABEL org.label-schema.description="Utilities for transforming data using dask and dask_cudf"

# Install conda
ENV PATH="/opt/conda/envs/dq/bin:/opt/conda/bin:$PATH"
RUN yum -y install wget && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh &&  ~/miniconda.sh -b -p /opt/conda && conda update conda

# conda package installs
RUN conda create -n dq python=3.8 && \
    conda install --name dq -c rapidsai -c nvidia -c conda-forge -c defaults \
    cudf=${RAPIDS_VERSION} cuml=${RAPIDS_VERSION} dask-cudf=${RAPIDS_VERSION} \
    dask-cuda=${RAPIDS_VERSION} cudatoolkit=${CUDA_VERSION} && \
    conda clean --all

# Project installs
WORKDIR /opt/dq
COPY ./ /opt/dq/
RUN pip install .

RUN chgrp -R 0 /opt/dq/ && \
    chmod -R g+rwX /opt/dq/

RUN pip install pytest

ENTRYPOINT ["pytest"]