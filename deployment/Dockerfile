FROM python:3.7
LABEL maintainer="mastley@spectric.com" \
      version="0.0.3"

COPY deployment/pip.conf /root/.pip/pip.conf

RUN mkdir -p /opt/elastic_datashader/tms_cache

COPY requirements.txt /opt/elastic_datashader

RUN pip install --upgrade pip && \
    pip install wheel && \
    pip install --no-cache-dir -r /opt/elastic_datashader/requirements.txt && \
    pip install gunicorn

COPY tms_datashader.py /opt/elastic_datashader
COPY tms_datashader_api /opt/elastic_datashader/tms_datashader_api
COPY templates /opt/elastic_datashader/templates
COPY deployment/gunicorn_config.py /opt/elastic_datashader

VOLUME ["/opt/elastic_datashader/tms-cache"]

ENTRYPOINT [ "gunicorn", \
    "--chdir", "/opt/elastic_datashader", \
    "-c", "/opt/elastic_datashader/gunicorn_config.py", \
    "--env", "DATASHADER_CACHE_DIR=/opt/elastic_datashader/tms-cache", \
    "tms_datashader:create_app()" \
]
