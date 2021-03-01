FROM docker.pkg.github.com/donchesworth/dask-quik/rapids_dask:latest

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
    chmod -R g+rwX /opt/dq/

ENTRYPOINT ["pytest"]