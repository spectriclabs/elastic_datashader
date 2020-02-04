FROM python:3
LABEL maintainer="mastley@spectric.com" \
      version="0.0.2"

COPY pip.conf /root/.pip/pip.conf

RUN mkdir -p /opt/elastic_datashader/tms_cache

ADD requirements.txt /opt/elastic_datashader

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/elastic_datashader/requirements.txt

ADD tms_pixellock.py /opt/elastic_datashader
ADD templates /opt/elastic_datashader/templates
ADD data_shader.patch /var/temp

RUN patch -p1 /usr/local/lib/python3.7/site-packages/datashader/transfer_functions < /var/tmp/data_shader.patch
VOLUME ["/opt/elastic_datashader/tms-cache"]

ENTRYPOINT ["/opt/elastic_datashader/tms_pixellock.py", "-f", "/opt/elastic_datashader/tms-cache/index_config.yaml", "-d", "/opt/elastic_datashader/tms-cache"]