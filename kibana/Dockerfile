FROM docker.elastic.co/kibana/kibana:7.8.0

USER root

RUN yum -y update && yum -y install git patch && yum -y clean all

USER kibana

RUN git clone -b feat-datashader-7.8.0 https://github.com/spectriclabs/kibana.git /tmp/kibana && \
    diff -ur /usr/share/kibana/x-pack/plugins/maps /tmp/kibana/build/kibana/x-pack/plugins/maps > /tmp/kibana.patch || true && \
    patch -d /usr/share/kibana -p 4 < /tmp/kibana.patch && \
    cp -R /tmp/kibana/build/kibana/x-pack/legacy/plugins/datashader_layer /usr/share/kibana/x-pack/legacy/plugins/ && \
    rm -rf /tmp/kibana /tmp/kibana.patch

RUN rm -rf /usr/share/kibana/optimize

RUN /usr/share/kibana/bin/kibana --optimize

