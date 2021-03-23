ARG CUDA_VERSION=10.2
ARG RAPIDS_VERSION=0.18
ARG OS_PLATFORM=ubi8
FROM nvidia/cuda:${CUDA_VERSION}-base-${OS_PLATFORM}

# Labels
LABEL maintainer="Don Chesworth<donald.chesworth@gmail.com>"
LABEL org.label-schema.schema-version="0.2"
LABEL org.label-schema.name="rapids-dask-ubi8"
LABEL org.label-schema.description="a small base image for using dask and dask_cudf on Red Hat"

# Install gcc, postgres, conda
ENV PATH="/opt/conda/envs/rd/bin:/opt/conda/bin:$PATH"
RUN yum install -y wget gcc gcc-c++ glibc-devel make postgresql-devel && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh &&  ~/miniconda.sh -b -p /opt/conda && conda update conda

# conda package installs
RUN conda create -n rd python=3.8 && \
    conda install --name rd -c rapidsai -c nvidia -c conda-forge -c defaults \
    cudf=${RAPIDS_VERSION} cuml=${RAPIDS_VERSION} dask-cudf=${RAPIDS_VERSION} \
    dask-cuda=${RAPIDS_VERSION} cudatoolkit=${CUDA_VERSION} && \
    conda clean --all

# Setup working dir
WORKDIR /opt/rd
RUN chgrp -R 0 /opt/rd/ && \
    chmod -R g+rwX /opt/rd/ && \
    mkdir datastorage && chown 1001 datastorage