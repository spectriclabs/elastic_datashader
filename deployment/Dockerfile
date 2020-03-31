FROM python:3.7
LABEL maintainer="mastley@spectric.com" \
      version="0.0.2"

COPY deployment/pip.conf /root/.pip/pip.conf

RUN mkdir -p /opt/elastic_datashader/tms_cache

ADD requirements.txt /opt/elastic_datashader

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/elastic_datashader/requirements.txt
RUN pip install gunicorn

ADD tms_pixellock.py /opt/elastic_datashader
ADD datashader_helpers.py /opt/elastic_datashader
ADD templates /opt/elastic_datashader/templates
ADD data_shader.patch /var/tmp

RUN patch -p1 -d /usr/local/lib/python3.7/site-packages/datashader/transfer_functions < /var/tmp/data_shader.patch
VOLUME ["/opt/elastic_datashader/tms-cache"]

COPY deployment/gunicorn_config.py /opt/elastic_datashader

ENTRYPOINT [ "gunicorn", \
    "--chdir", "/opt/elastic_datashader", \
    "-c", "/opt/elastic_datashader/gunicorn_config.py", \
    "--env", "DATASHADER_CACHE_DIR=/opt/elastic_datashader/tms-cache", \
    "tms_pixellock:create_app()" \
]
