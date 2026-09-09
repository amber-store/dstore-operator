IMG ?= ghcr.io/amber-store/dstore-operator:latest

CHART_VERSION ?= 0.1.6
HELM ?= helm

.PHONY: build test docker-build install deploy sample chart chart-push

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

chart:
	$(HELM) lint charts/dstore-operator
	$(HELM) package charts/dstore-operator --version $(CHART_VERSION) --app-version v$(CHART_VERSION)

chart-push: chart
	$(HELM) push dstore-operator-$(CHART_VERSION).tgz oci://ghcr.io/amber-store/charts
