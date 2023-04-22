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
RUN useradd -d /home/datashader datashader && \
    mkdir -p /home/datashader /opt/elastic_datashader/tms-cache && \
    chown -R datashader:datashader /home/datashader /opt/elastic_datashader

USER datashader
RUN mkdir /home/datashader/tmp
COPY --from=builder /build/dist/*.whl /home/datashader/tmp/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir /home/datashader/tmp/*.whl && \
    pip install uvicorn

COPY deployment/logging_config.yml /opt/elastic_datashader/

VOLUME ["/opt/elastic_datashader/tms-cache"]
ENV DATASHADER_CACHE_DIRECTORY=/opt/elastic_datashader/tms-cache

ENTRYPOINT [ "uvicorn", \
    "elastic_datashader:app", \
    "--ssl-ciphers","!SHA:!SHA256:!CHACHA20:!AESCCM:!ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384", \
    "--log-config", "/opt/elastic_datashader/logging_config.yml" \
]
