VERSION := $(shell grep version deployment/Dockerfile | perl -n -e '/version=\"(.+)\"/ && print $$1')
REPOSITORY :=
all:
	docker build -t $(REPOSITORY)elastic_datashader:$(VERSION) -f deployment/Dockerfile .
