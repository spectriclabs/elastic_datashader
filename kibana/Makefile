VERSION := $(shell grep version Dockerfile | perl -n -e '/version=\"(.+)\"/ && print $$1')
KIBANA_DIR := "../../kibana"

.PHONY: all no-cache

all:
	cp $(KIBANA_DIR)/target/kibana-7.10.1-SNAPSHOT-linux-x86_64.tar.gz .
	docker build -t $(REPOSITORY)kibana:$(VERSION) .

no-cache:
	docker build --no-cache -t $(REPOSITORY)kibana:$(VERSION) .
