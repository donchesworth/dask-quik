ARG CUDA_VERSION=10.2
ARG RAPIDS_VERSION=0.17
ARG OS_PLATFORM=ubuntu18.04
FROM nvidia/cuda:${CUDA_VERSION}-base-${OS_PLATFORM}

# Labels
LABEL maintainer="Don Chesworth<donald.chesworth@gmail.com>"
LABEL org.label-schema.schema-version="0.1"
LABEL org.label-schema.name="rapids_dask"
LABEL org.label-schema.description="a small base image for using dask and dask_cudf"

# Install conda
ENV PATH="/opt/conda/envs/dq/bin:/opt/conda/bin:$PATH"
RUN apt-get update && apt-get -y install wget && apt-get -y install git && \
    apt-get install -y --no-install-recommends build-essential && \
    apt-get install -y --no-install-recommends libpq-dev && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh &&  ~/miniconda.sh -b -p /opt/conda && conda update conda && \
    apt-get clean

# conda package installs
RUN conda create -n dq python=3.8 && \
    conda install --name dq -c rapidsai -c nvidia -c conda-forge -c defaults \
    cudf=${RAPIDS_VERSION} cuml=${RAPIDS_VERSION} dask-cudf=${RAPIDS_VERSION} \
    dask-cuda=${RAPIDS_VERSION} cudatoolkit=${CUDA_VERSION} && \
    conda clean --all

# Setup working dir
WORKDIR /opt/dq
RUN chgrp -R 0 /opt/dq/ && \
    chmod -R g+rwX /opt/dq/
