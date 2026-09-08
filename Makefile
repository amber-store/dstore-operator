IMG ?= ghcr.io/amber-store/dstore-operator:latest

.PHONY: build test docker-build install deploy sample

build:
	go build ./...

test:
	go test ./...

docker-build:
	docker build -t $(IMG) .

install:
	kubectl apply -f config/crd/

deploy: install
	kubectl apply -f config/manager/manager.yaml -f config/rbac/rbac.yaml

sample:
	kubectl apply -f config/samples/cluster.yaml
