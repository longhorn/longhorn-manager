package kubernetes

import (
	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func GetInClusterConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}

	// Fixes error: "GroupVersion is required when initializing a RESTClient"
	// Also need to allow pods/exec create permission to execute commands in pods.
	config.APIPath = "/api"
	config.GroupVersion = &schema.GroupVersion{Group: "", Version: "v1"}
	config.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: scheme.Codecs}

	return config, nil
}
