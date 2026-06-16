package steve

import (
	"context"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	apiServiceName     = "longhorn"
	apiServiceGroup    = "management.cattle.io"
	apiServiceVersion  = "v3"
	apiServiceResource = "apiservices"
)

var apiServiceGVR = schema.GroupVersionResource{
	Group:    apiServiceGroup,
	Version:  apiServiceVersion,
	Resource: apiServiceResource,
}

// CreateAPIService registers a management.cattle.io/v3 APIService so that
// Rancher routes requests matching the declared PathPrefixes through an
// aggregation tunnel to longhorn-manager's Steve handler. Rancher reacts by
// creating a Secret (SecretName/SecretNamespace) containing the tunnel
// credentials; StartAggregation watches that Secret and connects.
//
// management.cattle.io/v3.APIService is cluster-scoped, so the dynamic client
// call must NOT include a namespace.
func CreateAPIService(ctx context.Context, restConfig *rest.Config, secretNamespace string) error {
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiServiceGroup + "/" + apiServiceVersion,
			"kind":       "APIService",
			"metadata": map[string]interface{}{
				"name": apiServiceName,
			},
			"spec": map[string]interface{}{
				"secretName":      AggregationSecretName(),
				"secretNamespace": secretNamespace,
				"pathPrefixes":    []interface{}{"/v1/longhorn.io."},
			},
		},
	}

	existing, err := client.Resource(apiServiceGVR).Get(ctx, apiServiceName, metav1.GetOptions{})
	if err == nil {
		obj.SetResourceVersion(existing.GetResourceVersion())
		if _, err := client.Resource(apiServiceGVR).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
		logrus.Infof("Updated APIService %q (secretNamespace=%q)", apiServiceName, secretNamespace)
		return nil
	}

	if _, err := client.Resource(apiServiceGVR).Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		return err
	}
	logrus.Infof("Created APIService %q (secretNamespace=%q, pathPrefixes=[/v1/longhorn.io.])", apiServiceName, secretNamespace)
	return nil
}
