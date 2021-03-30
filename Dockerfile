FROM quay.io/donchesworth/rapids-dask-pytorch:py38-cuda10.2-rapids0.18-ubi8

# Labels
LABEL maintainer="Don Chesworth<donald.chesworth@gmail.com>"
LABEL org.label-schema.schema-version="0.1"
LABEL org.label-schema.name="dask-quik"
LABEL org.label-schema.description="Utilities for transforming data using dask and dask_cudf"

# Project installs
WORKDIR /opt/dq
COPY ./ /opt/dq/
RUN pip install .

RUN chgrp -R 0 /opt/dq/ && \
    chmod -R g+rwX /opt/dq/ && \
    chmod +x /opt/dq/entrypoint.sh

ENTRYPOINT ["/opt/dq/entrypoint.sh"]