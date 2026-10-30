GO ?= go
GOLANGCI_LINT_VERSION ?= v2.15.0
GOVULNCHECK_VERSION ?= v1.9.0
GOLANGCI_LINT ?= $(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

FUZZTIME ?= 30s
BENCHTIME ?= 1s
BENCHCOUNT ?= 1

.PHONY: all fmt fmt-check vet lint test race cover bench fuzz vuln

all: lint race

fmt:
	gofmt -w .

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...
	cd benchmarks && $(GO) vet ./...
	cd examples/std-middleware && $(GO) vet ./...

lint: fmt-check vet
	$(GOLANGCI_LINT) run ./...
	cd benchmarks && $(GOLANGCI_LINT) run ./...
	cd examples/std-middleware && $(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./...
	cd examples/std-middleware && $(GO) test ./...

race:
	$(GO) test -race ./...
	cd examples/std-middleware && $(GO) test -race ./...

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -n 1

bench:
	cd benchmarks && $(GO) test -run '^$$' -bench . -benchmem -benchtime $(BENCHTIME) -count $(BENCHCOUNT) ./...

fuzz:
	FUZZTIME=$(FUZZTIME) ./scripts/fuzz.sh

vuln:
	$(GOVULNCHECK) ./...
