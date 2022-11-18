FROM python:3.10 AS builder

ENV PIP_ROOT_USER_ACTION=ignore

RUN mkdir -p /build
RUN pip install --upgrade pip && \
    pip install poetry
COPY pyproject.toml /build/pyproject.toml
COPY README.md /build/README.md
COPY elastic_datashader /build/elastic_datashader
WORKDIR /build/elastic_datashader
RUN poetry build

FROM python:3.10 AS deployment
LABEL maintainer="foss@spectric.com"

ENV PIP_ROOT_USER_ACTION=ignore

COPY --from=builder /build/dist/*.whl /opt/elastic_datashader/
RUN mkdir -p /opt/elastic_datashader/tms-cache && \
    pip install --upgrade pip && \
    pip install --no-cache-dir /opt/elastic_datashader/*.whl && \
    pip install uvicorn

COPY deployment/logging_config.yml /opt/elastic_datashader

VOLUME ["/opt/elastic_datashader/tms-cache"]
ENV DATASHADER_CACHE_DIRECTORY=/opt/elastic_datashader/tms-cache

ENTRYPOINT [ "uvicorn", \
    "elastic_datashader:app", \
    "--log-config", "/opt/elastic_datashader/logging_config.yml" \
]
