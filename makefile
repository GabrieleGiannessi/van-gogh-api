# Variabili
DOCKERHUB_USER = gabrielegiannessi
IMAGE_NAME = $(DOCKERHUB_USER)/van-gogh-dna-fastapi
VERSION = $(shell grep '^version =' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

.PHONY: build push publish

build:
	docker build -t $(IMAGE_NAME):$(VERSION) .
	docker tag $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):latest

push: 
	docker push $(IMAGE_NAME):$(VERSION)
	docker push $(IMAGE_NAME):latest

publish: build push