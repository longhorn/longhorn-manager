package storageclass

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSecretParameters(t *testing.T) {
	params := map[string]string{
		"csi.storage.k8s.io/provisioner-secret-name":             "",
		"csi.storage.k8s.io/provisioner-secret-namespace":        "namespace",
		"csi.storage.k8s.io/controller-publish-secret-name":      "name",
		"csi.storage.k8s.io/controller-publish-secret-namespace": "namespace",
		"csi.storage.k8s.io/controller-expand-secret-name":       "name",
		"csi.storage.k8s.io/controller-expand-secret-namespace":  "namespace",
		"csi.storage.k8s.io/controller-custom-secret-token":      "token",
		"csi.storage.k8s.io/secret-name":                         "name",
		"csi.storage.k8s.io/secret-namespace":                    "namespace",
		"csiProvisionerSecretName":                               "name",
		"csiProvisionerSecretNamespace":                          "namespace",
		"csiControllerPublishSecretName":                         "name",
		"csiControllerPublishSecretNamespace":                    "namespace",
		"csi.storage.k8s.io/node-stage-secret-name":              "name",
		"csi.storage.k8s.io/node-stage-secret-namespace":         "namespace",
		"csi.storage.k8s.io/node-publish-secret-name":            "name",
		"csi.storage.k8s.io/node-publish-secret-namespace":       "namespace",
		"csi.storage.k8s.io/node-expand-secret-name":             "name",
		"csi.storage.k8s.io/node-expand-secret-namespace":        "namespace",
		"csi.storage.k8s.io/controller-secret-name":              "name",
		"other.csi.example/secret-name":                          "name",
	}

	expectedRejected := map[string]struct{}{
		"csi.storage.k8s.io/provisioner-secret-name":             {},
		"csi.storage.k8s.io/provisioner-secret-namespace":        {},
		"csi.storage.k8s.io/controller-publish-secret-name":      {},
		"csi.storage.k8s.io/controller-publish-secret-namespace": {},
		"csi.storage.k8s.io/controller-expand-secret-name":       {},
		"csi.storage.k8s.io/controller-expand-secret-namespace":  {},
		"csi.storage.k8s.io/controller-custom-secret-token":      {},
		"csi.storage.k8s.io/secret-name":                         {},
		"csi.storage.k8s.io/secret-namespace":                    {},
		"csiProvisionerSecretName":                               {},
		"csiProvisionerSecretNamespace":                          {},
		"csiControllerPublishSecretName":                         {},
		"csiControllerPublishSecretNamespace":                    {},
	}

	errs := validateSecretParameters(params)
	require.Len(t, errs, len(expectedRejected))

	for _, err := range errs {
		key, ok := err.BadValue.(string)
		require.True(t, ok)
		_, found := expectedRejected[key]
		require.True(t, found, "unexpected rejected parameter %q", key)
		delete(expectedRejected, key)
	}
	require.Empty(t, expectedRejected)
}
