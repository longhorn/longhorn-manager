#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

LH_MANAGER_DIR="github.com/longhorn/longhorn-manager"
OUTPUT_DIR="${LH_MANAGER_DIR}/k8s/pkg/client"
APIS_DIR="${LH_MANAGER_DIR}/k8s/pkg/apis"
GROUP_VERSION="longhorn:v1beta1"
CODE_GENERATOR_VERSION="v0.18.0"
CRDS_DIR="crds"
CONTROLLER_TOOLS_VERSION="v0.7.0"

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

if ! command -v controller-gen > /dev/null; then
  echo "controller-gen is missing"
  echo "Prepare to install controller-gen"
  go install sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_TOOLS_VERSION}
fi

# https://github.com/kubernetes-sigs/controller-tools/tree/v0.7.0/cmd/controller-gen
${GOPATH}/src/k8s.io/code-generator/generate-groups.sh \
  deepcopy,client,lister,informer \
  ${OUTPUT_DIR} \
  ${APIS_DIR} \
  ${GROUP_VERSION} \
  $@

echo Generating CRD
controller-gen crd paths=${APIS_DIR}/... output:crd:dir=${CRDS_DIR}
for crd in ${CRDS_DIR}/*.yaml; do
  cat $crd > deploy/install/01-prerequisite/03-crd.yaml
done
