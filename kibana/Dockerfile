FROM docker.elastic.co/kibana/kibana:7.10.1
LABEL source="https://github.com/spectriclabs/kibana/tree/feat-datashader-7.6.2" \
      version="7.10.1-datashader-0.0.1"

USER root

RUN yum -y update && yum -y install patch && yum -y clean all

USER kibana

COPY kibana-7.10.1-SNAPSHOT-linux-x86_64.tar.gz /tmp

RUN tar -C /usr/share/kibana --strip-components=1 -xzf /tmp/kibana-7.10.1-SNAPSHOT-linux-x86_64.tar.gz

RUN curl https://registry.npmjs.org/mgrs/-/mgrs-1.0.0.tgz -o /tmp/mgrs-1.0.0.tgz && \
    mkdir -p /usr/share/kibana/x-pack/node_modules/mgrs && \
    tar -xvzf /tmp/mgrs-1.0.0.tgz --strip-components=1 -C /usr/share/kibana/x-pack/node_modules/mgrs && \
    rm /tmp/mgrs-1.0.0.tgz

RUN curl https://registry.npmjs.org/utm/-/utm-1.1.1.tgz -o /tmp/utm-1.1.1.tgz && \
    mkdir -p /usr/share/kibana/x-pack/node_modules/utm && \
    tar -xvzf /tmp/utm-1.1.1.tgz --strip-components=1 -C /usr/share/kibana/x-pack/node_modules/utm && \
    rm /tmp/utm-1.1.1.tgz

RUN rm -rf /usr/share/kibana/optimize

RUN /usr/share/kibana/bin/kibana --optimize
