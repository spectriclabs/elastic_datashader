FROM docker.elastic.co/kibana/kibana:7.6.0

USER root

RUN yum -y update && yum -y install git patch && yum -y clean all

USER kibana

RUN git clone -b feat-datashader https://github.com/spectriclabs/kibana.git /tmp/kibana && \
    cd /tmp/kibana && \
    git fetch --tags && \
    git diff v7.6.0..HEAD > /tmp/kibana.patch && \
    patch -d /usr/share/kibana -p 1 < /tmp/kibana.patch && \
    rm -rf /tmp/kibana /tmp/kibana.patch

RUN rm -rf /usr/share/kibana/optimize

RUN /usr/share/kibana/bin/kibana --optimize

