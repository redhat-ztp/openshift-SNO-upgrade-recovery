#!/usr/bin/env bash
# single test: go test -v ./pkg/storage/
# without cache: go test -count=1 -v ./pkg/storage/
set -e -x
echo "Linting go code..."
golint ./cmd ./pkg
make verify
make golangci-lint
echo "Running go tests..."
find . -name go.mod -not -path "*/vendor/*" -execdir go test -v -covermode=count -coverprofile=coverage.out  ./... \;
