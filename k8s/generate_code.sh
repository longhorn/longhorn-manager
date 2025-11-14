#!/bin/bash

# This script only works with go v1.17 or above

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(cd $(dirname "${BASH_SOURCE[0]}")/.. && pwd)
LH_MANAGER_PKG="github.com/longhorn/longhorn-manager"
OUTPUT_PKG="${LH_MANAGER_PKG}/k8s/pkg/client"
APIS_PATH="k8s/pkg/apis"
APIS_DIR="${SCRIPT_ROOT}/${APIS_PATH}"
GROUP_VERSION="longhorn:v1beta2"
CODE_GENERATOR_VERSION="v0.34.0"
CRDS_DIR="crds"
CONTROLLER_TOOLS_VERSION="v0.17.1"
KUSTOMIZE_VERSION="v5.6.0"
GOPATH="${GOPATH:-}"


function check_code_version() {
  local repo_path="$1"
  local expected_version="$2"

  pushd "${repo_path}" >/dev/null

  local tags
  tags=$(git tag --points-at HEAD 2>/dev/null)

  popd >/dev/null

  if [[ -z "${tags}" ]]; then
    echo "No tags found on current commit"
    return 1
  fi

  for tag in ${tags}; do
    # Normalize tag: remove kubernetes- prefix
    local stripped="${tag#kubernetes-}"

    if [[ "${stripped}" == "${expected_version}" ]]; then
      return 0
    fi
  done

  echo "code-generator version mismatch:"
  echo "  found tags: ${tags}"
  echo "  expected:   ${expected_version}"
  return 1
}

if [[ -z "$GOPATH" ]]; then
  GOPATH="$(go env GOPATH)"

  if [[ -z "$GOPATH" ]]; then
    echo "GOPATH is not set"
    exit 1
  fi
fi

# https://github.com/kubernetes/code-generator/blob/${CODE_GENERATOR_VERSION}/generate-groups.sh
if [[ -d "${GOPATH}/src/k8s.io/code-generator" ]]; then
  if ! check_code_version "${GOPATH}/src/k8s.io/code-generator" "${CODE_GENERATOR_VERSION}"; then
    exit 1
  fi
else
  echo "${GOPATH}/src/k8s.io/code-generator is missing"
  echo "Prepare to install code-generator"
	mkdir -p ${GOPATH}/src/k8s.io
	pushd ${GOPATH}/src/k8s.io
	git clone -b ${CODE_GENERATOR_VERSION} https://github.com/kubernetes/code-generator 2>/dev/null || true
  cd code-generator
  go mod tidy
  go mod vendor
	popd
fi

# https://github.com/kubernetes-sigs/controller-tools/tree/${CONTROLLER_TOOLS_VERSION}/cmd/controller-gen
if ! command -v controller-gen > /dev/null; then
  echo "controller-gen is missing"
  echo "Prepare to install controller-gen"
  GOFLAGS= go install sigs.k8s.io/controller-tools/cmd/controller-gen@${CONTROLLER_TOOLS_VERSION}
else
  # Execute `controller-gen` to ensure the version is correct
  CONTROLLER_GEN_VERSION_OUTPUT=$(controller-gen --version 2>&1 | awk '{print $2}')
  if ! echo "${CONTROLLER_GEN_VERSION_OUTPUT}" | grep -q "${CONTROLLER_TOOLS_VERSION}"; then
    echo "controller-gen version mismatch:"
    echo "  found:    ${CONTROLLER_GEN_VERSION_OUTPUT}"
    echo "  expected: ${CONTROLLER_TOOLS_VERSION}"
    exit 1
  fi
fi

# https://github.com/kubernetes-sigs/kustomize/tree/kustomize/${KUSTOMIZE_VERSION}/kustomize
if ! command -v kustomize > /dev/null; then
  echo "kustomize is missing"
  echo "Prepare to install kustomize"
  GOFLAGS= go install sigs.k8s.io/kustomize/kustomize/v5@${KUSTOMIZE_VERSION}
fi

# Generate clientset, informers, listers, ...
source ${GOPATH}/src/k8s.io/code-generator/kube_codegen.sh

kube::codegen::gen_client \
  --with-watch \
  --with-applyconfig \
  --output-dir "${SCRIPT_ROOT}/k8s/pkg/client" \
  --output-pkg "${OUTPUT_PKG}" \
  --boilerplate "${SCRIPT_ROOT}/k8s/hack/boilerplate.go.txt" \
  "${SCRIPT_ROOT}/k8s/pkg/apis"

kube::codegen::gen_helpers \
  --boilerplate "${SCRIPT_ROOT}/k8s/hack/boilerplate.go.txt" \
  "${SCRIPT_ROOT}/k8s/pkg/apis"

echo Generating CRD
controller-gen crd paths=${APIS_DIR}/... output:crd:dir=${CRDS_DIR}
pushd ${CRDS_DIR}
kustomize create --autodetect 2>/dev/null || true
kustomize edit add label longhorn-manager: 2>/dev/null || true
if [ -e ${SCRIPT_ROOT}/k8s/patches/crd ]; then
  cp -a ${SCRIPT_ROOT}/k8s/patches/crd patches
  find patches -type f | xargs -i sh -c 'kustomize edit add patch --path {}'
fi
popd

rm -rf ${SCRIPT_ROOT}/k8s/crds.yaml
echo "# Generated crds.yaml from ${LH_MANAGER_PKG}/${APIS_PATH} and the crds.yaml will be copied to longhorn/longhorn chart/templates and cannot be directly used by kubectl apply." > ${SCRIPT_ROOT}/k8s/crds.yaml
kustomize build ${CRDS_DIR} >> ${SCRIPT_ROOT}/k8s/crds.yaml
rm -r ${CRDS_DIR}

# Replace labels and namespace in crds.yaml. The crds.yaml is used by helm chart and used for generating the installation manifests.
echo "Replacing labels and namespace in crds.yaml..."
sed -i 's/^  labels:/  labels: {{- include "longhorn.labels" . | nindent 4 }}/g' ${SCRIPT_ROOT}/k8s/crds.yaml
sed -i 's/^          namespace: longhorn-system/          namespace: {{ include "release_namespace" . }}/g' ${SCRIPT_ROOT}/k8s/crds.yaml
