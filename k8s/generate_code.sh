#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

LH_MANAGER_DIR="github.com/longhorn/longhorn-manager"
OUTPUT_DIR="${LH_MANAGER_DIR}/k8s/pkg/client"
APIS_DIR="${LH_MANAGER_DIR}/k8s/pkg/apis"
GROUP_VERSION="longhorn:v1beta1,v1beta2"
CODE_GENERATOR_VERSION="v0.18.0"
CRDS_DIR="crds"
CONTROLLER_TOOLS_VERSION="v0.7.0"
KUSTOMIZE_VERSION="kustomize/v3.10.0"

if [[ -z "${GOPATH}" ]]; then
  GOPATH=~/go
fi

# https://github.com/kubernetes/code-generator/blob/v0.18.0/generate-groups.sh
if [[ ! -d "${GOPATH}/src/k8s.io/code-generator" ]]; then
  echo "${GOPATH}/src/k8s.io/code-generator is missing"
  echo "Prepare to install code-generator"
	mkdir -p ${GOPATH}/src/k8s.io
	pushd ${GOPATH}/src/k8s.io
	git clone -b ${CODE_GENERATOR_VERSION} https://github.com/kubernetes/code-generator 2>/dev/null || true
	popd
fi

# https://github.com/kubernetes-sigs/controller-tools/tree/v0.7.0/cmd/controller-gen
if ! command -v controller-gen > /dev/null; then
  echo "controller-gen is missing"
  echo "Prepare to install controller-gen"
  go install sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_TOOLS_VERSION}
fi

# https://github.com/kubernetes-sigs/kustomize/tree/kustomize/v3.10.0/kustomize
if ! command -v kustomize > /dev/null; then
  echo "kustomize is missing"
  echo "Prepare to install kustomize"
	mkdir -p ${GOPATH}/src/github.com/kubernetes-sigs
	pushd ${GOPATH}/src/github.com/kubernetes-sigs
	git clone -b ${KUSTOMIZE_VERSION} git@github.com:kubernetes-sigs/kustomize.git 2>/dev/null || true
	cd kustomize/kustomize
	go install .
	popd
fi

bash ${GOPATH}/src/k8s.io/code-generator/generate-groups.sh \
  deepcopy,client,lister,informer \
  ${OUTPUT_DIR} \
  ${APIS_DIR} \
  ${GROUP_VERSION} \
  $@

echo Generating CRD
controller-gen crd paths=${APIS_DIR}/... output:crd:dir=${CRDS_DIR}
pushd ${CRDS_DIR}
kustomize create --autodetect 2>/dev/null || true
kustomize edit add label longhorn-manager: 2>/dev/null || true
popd
kustomize build ${CRDS_DIR} > ${GOPATH}/src/${LH_MANAGER_DIR}/deploy/install/01-prerequisite/03-crd.yaml
rm -r ${CRDS_DIR}
