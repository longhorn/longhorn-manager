PROJECT := longhorn-manager
MACHINE := longhorn
BUILD_DOCKER_ARGS :=
ifdef LONGHORN_TWO_MINOR_UPGRADE_DISTROS
BUILD_DOCKER_ARGS += --build-arg LONGHORN_TWO_MINOR_UPGRADE_DISTROS=$(LONGHORN_TWO_MINOR_UPGRADE_DISTROS)
endif

# Resolve git metadata on the host and pass it into the build. Inside the build
# container `git` does not work when the checkout is a git worktree (or a jj
# workspace), because .git is a file pointing to a path outside the build context.
# scripts/version falls back to running git itself when these are not provided.
GIT_COMMIT := $(shell git rev-parse HEAD 2>/dev/null)
GIT_COMMIT_SHORT := $(shell git rev-parse --short HEAD 2>/dev/null)
GIT_DIRTY := $(shell test -n "$$(git status --porcelain --untracked-files=no 2>/dev/null)" && echo -dirty)
GIT_TAG := $(shell git tag -l --contains HEAD 2>/dev/null | head -n 1)
COMMIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
COMMIT_TAG := $(shell git tag --points-at HEAD 2>/dev/null | head -n 1)
BUILD_DOCKER_ARGS += --build-arg GIT_COMMIT=$(GIT_COMMIT)
BUILD_DOCKER_ARGS += --build-arg GIT_COMMIT_SHORT=$(GIT_COMMIT_SHORT)
BUILD_DOCKER_ARGS += --build-arg GIT_DIRTY=$(GIT_DIRTY)
BUILD_DOCKER_ARGS += --build-arg GIT_TAG=$(GIT_TAG)
BUILD_DOCKER_ARGS += --build-arg COMMIT_BRANCH=$(COMMIT_BRANCH)
BUILD_DOCKER_ARGS += --build-arg COMMIT_TAG=$(COMMIT_TAG)

# Define the target platforms that can be used across the ecosystem.
# Note that what would actually be used for a given project will be
# defined in TARGET_PLATFORMS, and must be a subset of the below:
DEFAULT_PLATFORMS := linux/amd64,linux/arm64

.PHONY: build validate test ci package
build:
	docker buildx build $(BUILD_DOCKER_ARGS) --target build-artifacts --output type=local,dest=. -f Dockerfile .

validate:
	docker buildx build --target validate -f Dockerfile .

test:
	docker buildx build --target base -t longhorn-manager-test -f Dockerfile .
	docker run --privileged -v /dev:/host/dev longhorn-manager-test ./scripts/test

ci:
	docker buildx build $(BUILD_DOCKER_ARGS) --target ci-artifacts --output type=local,dest=. -f Dockerfile .

package:
	bash scripts/package

.PHONY: buildx-machine
buildx-machine:
	@docker buildx create --name=$(MACHINE) --platform=$(DEFAULT_PLATFORMS) 2>/dev/null || true
	docker buildx inspect $(MACHINE)

# variables needed from GHA caller:
# - REPO: image repo, include $registry/$repo_path
# - TAG: image tag
# - TARGET_PLATFORMS: optional, to be passed for buildx's --platform option
# - IID_FILE_FLAG: optional, options to generate image ID file
.PHONY: workflow-image-build-push workflow-image-build-push-secure
workflow-image-build-push: buildx-machine
	MACHINE=$(MACHINE) PUSH='true' IMAGE_NAME=$(PROJECT) bash scripts/package
workflow-image-build-push-secure: buildx-machine
	MACHINE=$(MACHINE) PUSH='true' IMAGE_NAME=$(PROJECT) IS_SECURE=true bash scripts/package

generate:
	bash k8s/generate_code.sh

.DEFAULT_GOAL := ci
