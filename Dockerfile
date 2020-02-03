FROM python:3
LABEL maintainer="mastley@spectric.com" \
      version="0.0.2"

COPY pip.conf /root/.pip/pip.conf

RUN mkdir -p /opt/elastic_datashader/tms_cache

ADD requirements.txt /opt/elastic_datashader

RUN pip install --no-cache-dir -r /opt/elastic_datashader/requirements.txt

ADD tms_pixellock.py /opt/elastic_datashader
ADD templates /opt/elastic_datashader/templates
ADD runtms_docker.sh /opt/elastic_datashader

VOLUME ["/opt/elastic_datashader/tms-cache"]

CMD ["/opt/elastic_datashader/tms_pixellock.py", "-f /opt/elastic_datashader/tms-cache/index_config.yaml", "-d /opt/elastic_datashader/tms-cache"]