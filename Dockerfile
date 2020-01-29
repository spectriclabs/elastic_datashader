FROM python:3
LABEL maintainer="mastley@spectric.com" \
      version="0.0.1"

RUN mkdir -p /opt/elastic_datashader/tms_cache

ADD requirements.txt /opt/elastic_datashader

RUN pip install --no-cache-dir -r /opt/elastic_datashader/requirements.txt

ADD tms_pixellock.py /opt/elastic_datashader
ADD templates /opt/elastic_datashader/templates
ADD runtms_docker.sh /opt/elastic_datashader

VOLUME ["/opt/elastic_datashader/tms-cache"]

ENTRYPOINT ["/opt/elastic_datashader/runtms_docker.sh"]