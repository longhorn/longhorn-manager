TARGETS := $(shell ls scripts)
MACHINE := rancher
DEFAULT_PLATFORMS := linux/amd64,linux/arm64,darwin/arm64,darwin/amd64

.dapper:
	@echo Downloading dapper
	@curl -sL https://releases.rancher.com/dapper/latest/dapper-`uname -s`-`uname -m` > .dapper.tmp
	@@chmod +x .dapper.tmp
	@./.dapper.tmp -v
	@mv .dapper.tmp .dapper

$(TARGETS): .dapper
	./.dapper $@
generate:
	bash k8s/generate_code.sh

.PHONY: workflow-buildx-machine
workflow-buildx-machine:
	docker buildx ls | grep $(MACHINE) >/dev/null || \
		docker buildx create --name=$(MACHINE) --platform=$(DEFAULT_PLATFORMS)

# variables read from GHA:
# - REPO: image repo, include $registry/$repo_path
# - TAG: image tag
# - TARGET_PLATFORMS: to be passed for buildx's --platform option
# - IID_FILE_FLAG: options to generate image ID file
.PHONY: workflow-image-build-push-community workflow-image-build-push-secure
workflow-image-build-push-community: build workflow-buildx-machine
	MACHINE=$(MACHINE) bash script/image-build-push
workflow-image-build-push-secure: build workflow-buildx-machine
	MACHINE=$(MACHINE) bash script/image-build-push secure

.DEFAULT_GOAL := ci

.PHONY: $(TARGETS)
