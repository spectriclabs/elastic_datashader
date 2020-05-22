VERSION := $(shell grep version deployment/Dockerfile | perl -n -e '/version=\"(.+)\"/ && print $$1')
REPOSITORY :=

all: elastic_datashader elastic_datashader_kibana
.PHONY: all

elastic_datashader:
	docker build -t $(REPOSITORY)elastic_datashader:$(VERSION) -f deployment/Dockerfile .

elastic_datashader_kibana:
	docker build -t $(REPOSITORY)elastic_datashader_kibana:$(VERSION) -f kibana/Dockerfile kibana/
