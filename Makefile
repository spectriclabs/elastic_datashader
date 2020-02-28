VERSION := $(shell grep version deployment/Dockerfile | perl -n -e '/version=\"(.+)\"/ && print $$1')

all:
	docker build -t elastic_datashader:$(VERSION) -f deployment/Dockerfile .
