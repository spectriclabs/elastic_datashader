FROM docker.elastic.co/kibana/kibana:7.6.2
LABEL source="https://github.com/spectriclabs/kibana/tree/feat-datashader-7.6.2" \
      version="7.6.2-datashader-0.0.1"

USER root

RUN yum -y update && yum -y install git patch && yum -y clean all

USER kibana

RUN git clone -b feat-datashader-7.6.2 https://github.com/spectriclabs/kibana.git /tmp/kibana 

RUN diff -ur /usr/share/kibana/x-pack/legacy/plugins/maps /tmp/kibana/x-pack/legacy/plugins/maps > /tmp/kibana.patch || true && \
    patch -d /usr/share/kibana -p 4 < /tmp/kibana.patch && \
    cp -R /tmp/kibana/x-pack/legacy/plugins/datashader_layer /usr/share/kibana/x-pack/legacy/plugins/ && \
    rm -rf /tmp/kibana /tmp/kibana.patch

RUN curl https://registry.npmjs.org/mgrs/-/mgrs-1.0.0.tgz -o /tmp/mgrs-1.0.0.tgz && \
    mkdir -p /usr/share/kibana/x-pack/node_modules/mgrs && \
    tar -xvzf /tmp/mgrs-1.0.0.tgz --strip-components=1 -C /usr/share/kibana/x-pack/node_modules/mgrs && \
    rm /tmp/mgrs-1.0.0.tgz

RUN curl https://registry.npmjs.org/utm/-/utm-1.1.1.tgz -o /tmp/utm-1.1.1.tgz && \
    mkdir -p /usr/share/kibana/x-pack/node_modules/utm && \
    tar -xvzf /tmp/utm-1.1.1.tgz --strip-components=1 -C /usr/share/kibana/x-pack/node_modules/utm && \
    rm /tmp/utm-1.1.1.tgz

ADD schema.js.patch /tmp/schema.js.patch

RUN patch -d /usr/share/kibana/src/legacy/server/config < /tmp/schema.js.patch

RUN rm -rf /usr/share/kibana/optimize

RUN /usr/share/kibana/bin/kibana --optimize
