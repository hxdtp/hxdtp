.PHONY: help test deps clean

# Ref: https://gist.github.com/prwhite/8168133
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

GOLANGCI_LINT_VERSION ?= "latest"

test:  ## Run test cases. (Args: GOLANGCI_LINT_VERSION=latest)
	if [ ! -e ./bin/golangci-lint ]; then \
		curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s $(GOLANGCI_LINT_VERSION); \
	fi
	./bin/golangci-lint run
	go test -v -race -coverprofile=coverage.out ./...


deps:  ## Update vendor.
	go mod verify
	go mod tidy -v
	rm -rf vendor
	go mod vendor -v


clean:  ## Clean up useless files.
	rm -rf bin
	find . -type f -name '*.out' -exec rm -f {} +
	find . -type f -name '.DS_Store' -exec rm -f {} +
	find . -type f -name '*.test' -exec rm -f {} +
	find . -type f -name '*.prof' -exec rm -f {} +
	find . -type s -name 'localhost:*' -exec rm -f {} +
	find . -type s -name '127.0.0.1:*' -exec rm -f {} +
