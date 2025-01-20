#!/bin/bash

# This script only works with go v1.17 or above

set -o errexit
set -o nounset
set -o pipefail

LH_MANAGER_DIR="github.com/longhorn/longhorn-manager"
OUTPUT_DIR="${LH_MANAGER_DIR}/k8s/pkg/client"
APIS_DIR="${LH_MANAGER_DIR}/k8s/pkg/apis"
GROUP_VERSION="longhorn:v1beta1,v1beta2"
CODE_GENERATOR_VERSION="v0.32.1"
CRDS_DIR="crds"
CONTROLLER_TOOLS_VERSION="v0.17.1"
KUSTOMIZE_VERSION="v5.6.0"
GOPATH="${GOPATH:-}"


if [[ -z "$GOPATH" ]]; then
  GOPATH="$(go env GOPATH)"

  if [[ -z "$GOPATH" ]]; then
    echo "GOPATH is not set"
    exit 1
  fi
fi

# https://github.com/kubernetes/code-generator/blob/${CODE_GENERATOR_VERSION}/generate-groups.sh
if [[ ! -d "${GOPATH}/src/k8s.io/code-generator" ]]; then
  echo "${GOPATH}/src/k8s.io/code-generator is missing"
  echo "Prepare to install code-generator"
	mkdir -p ${GOPATH}/src/k8s.io
	pushd ${GOPATH}/src/k8s.io
	git clone -b ${CODE_GENERATOR_VERSION} https://github.com/kubernetes/code-generator 2>/dev/null || true
	popd
fi

# https://github.com/kubernetes-sigs/controller-tools/tree/${CONTROLLER_TOOLS_VERSION}/cmd/controller-gen
if ! command -v controller-gen > /dev/null; then
  echo "controller-gen is missing"
  echo "Prepare to install controller-gen"
  GOFLAGS= go install sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_TOOLS_VERSION}
fi

# https://github.com/kubernetes-sigs/kustomize/tree/kustomize/${KUSTOMIZE_VERSION}/kustomize
if ! command -v kustomize > /dev/null; then
  echo "kustomize is missing"
  echo "Prepare to install kustomize"
  GOFLAGS= go install sigs.k8s.io/kustomize/kustomize/v5@${KUSTOMIZE_VERSION}
fi

# The generators use GOPATH when locating boilerplate.go.txt, so it must be made available in the child shell.
export GOPATH
bash ${GOPATH}/src/k8s.io/code-generator/kube_codegen.sh \
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
if [ -e ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/patches/crd ]; then
  cp -a ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/patches/crd patches
  find patches -type f | xargs -i sh -c 'kustomize edit add patch --path {}'
fi
popd

rm -rf ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/crds.yaml
echo "# Generating crds.yaml from ${APIS_DIR} and the crds.yaml will be copied to longhorn/longhorn chart/templates and cannot be directly used by kubectl apply." > ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/crds.yaml
kustomize build ${CRDS_DIR} >> ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/crds.yaml
rm -r ${CRDS_DIR}

# Replace labels and namespace in crds.yaml. The crds.yaml is used by helm chart and used for generating the installation manifests.
echo "Replacing labels and namespace in crds.yaml..."
sed -i 's/^  labels:/  labels: {{- include "longhorn.labels" . | nindent 4 }}/g' ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/crds.yaml
sed -i 's/^          namespace: longhorn-system/          namespace: {{ include "release_namespace" . }}/g' ${GOPATH}/src/${LH_MANAGER_DIR}/k8s/crds.yaml
