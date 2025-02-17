SHELL :=/bin/bash
export GO111MODULE=on
unexport GOPATH

include $(addprefix ./vendor/github.com/openshift/build-machinery-go/make/, \
	golang.mk \
	targets/openshift/deps-gomod.mk \
	targets/openshift/deps.mk \
	targets/openshift/images.mk \
)

CONTAINER_COMMAND = $(shell if [ -x "$(shell which podman)" ];then echo "podman" ; else echo "docker";fi)
IMAGE := $(or ${IMAGE},quay.io/redhat_ztp/openshift-sno-upgrade-recovery:latest)
GIT_REVISION := $(shell git rev-parse HEAD)
CONTAINER_BUILD_PARAMS = --label git_revision=${GIT_REVISION}

all: build build-image
.PHONY: all

build:
	hack/build-go.sh
.PHONY: build

build-image: build
	$(CONTAINER_COMMAND) build $(CONTAINER_BUILD_PARAMS) -f backup.Dockerfile . -t $(IMAGE)
.PHONY:

push-image: build-image
	$(CONTAINER_COMMAND) push ${IMAGE}

.PHONY: build

check: | verify golangci-lint check-shellcheck check-bashate check-markdownlint
.PHONY: check

golangci-lint:
		golangci-lint run --verbose --print-resources-usage --modules-download-mode=vendor --timeout=5m0s
.PHONY: golangci-lint

ifeq ($(shell which shellcheck 2>/dev/null),)
check-shellcheck:
	@echo "Skipping shellcheck: Not installed"
else
check-shellcheck:
	find . -name '*.sh' -not -path '*/vendor/*' -not -path './git/*' -print0 \
		| xargs -0 --no-run-if-empty shellcheck
endif
.PHONY: check-shellcheck

ifeq ($(shell which bashate 2>/dev/null),)
check-bashate:
	@echo "Skipping bashate: Not installed"
else
# Ignored bashate errors/warnings:
#   E006 Line too long
check-bashate:
	find . -name '*.sh' -not -path '*/vendor/*' -not -path './git/*' -print0 \
		| xargs -0 --no-run-if-empty bashate -e 'E*' -i E006
endif
.PHONY: check-bashate

ifeq ($(shell which markdownlint 2>/dev/null),)
check-markdownlint:
	@echo "Skipping markdownlint: Not installed"
else
check-markdownlint:
	find . -name '*.md' -not -path '*/vendor/*' -not -path './git/*' -print0 \
		| xargs -0 --no-run-if-empty markdownlint
endif
.PHONY: check-markdownlint


GO_TEST_PACKAGES :=./pkg/... ./cmd/...
