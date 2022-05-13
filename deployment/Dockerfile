FROM python:3.9 AS builder

RUN mkdir -p /build
RUN pip install --upgrade pip && \
    pip install poetry
COPY pyproject.toml /build/pyproject.toml
COPY README.md /build/README.md
COPY elastic_datashader /build/elastic_datashader
WORKDIR /build/elastic_datashader
RUN poetry build

FROM python:3.9 AS deployment
LABEL maintainer="foss@spectric.com" \
      version="0.0.5"

RUN mkdir -p /opt/elastic_datashader/tms-cache
COPY --from=builder /build/dist/*.whl /opt/elastic_datashader/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir /opt/elastic_datashader/*.whl && \
    pip install uvicorn

COPY deployment/logging_config.yml /opt/elastic_datashader

VOLUME ["/opt/elastic_datashader/tms-cache"]
ENV DATASHADER_CACHE_DIR=/opt/elastic_datashader/tms-cache

ENTRYPOINT [ "uvicorn", \
    "elastic_datashader:app", \
    "--log-config", "/opt/elastic_datashader/logging_config.yml"
]
