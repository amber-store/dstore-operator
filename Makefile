IMG ?= ghcr.io/amber-store/dstore-operator:latest
NODE_IMG ?= ghcr.io/amber-store/dstore:latest
DSTORE_VERSION ?= main

.PHONY: build test docker-build docker-build-node install deploy sample

build:
	go build ./...

test:
	go test ./...

docker-build:
	docker build -t $(IMG) .

docker-build-node:
	docker build --build-arg DSTORE_VERSION=$(DSTORE_VERSION) -t $(NODE_IMG) images/dstore

install:
	kubectl apply -f config/crd/

deploy: install
	kubectl apply -f config/manager/manager.yaml -f config/rbac/rbac.yaml

sample:
	kubectl apply -f config/samples/cluster.yaml
