#!/bin/bash

APIS_DIR="github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn"
VERSION="v1alpha1"
APIS_VERSION_DIR="${APIS_DIR}/${VERSION}"
OUTPUT_DIR="github.com/rancher/longhorn-manager/k8s/pkg/client"
CLIENTSET_DIR="${OUTPUT_DIR}/clientset"
LISTERS_DIR="${OUTPUT_DIR}/listers"
INFORMERS_DIR="${OUTPUT_DIR}/informers"

echo Generating deepcopy
deepcopy-gen --input-dirs ${APIS_VERSION_DIR} \
	-O zz_generated.deepcopy --bounding-dirs ${APIS_DIR}

echo Generating clientset
client-gen --clientset-name versioned \
	--input-base '' --input ${APIS_VERSION_DIR} \
	--clientset-path ${CLIENTSET_DIR}

echo Generating lister
lister-gen --input-dirs ${APIS_VERSION_DIR} \
	--output-package ${LISTERS_DIR}

echo Generating informer
informer-gen --input-dirs ${APIS_VERSION_DIR} \
	--versioned-clientset-package "${CLIENTSET_DIR}/versioned" \
	--listers-package ${LISTERS_DIR} \
	--output-package ${INFORMERS_DIR}

