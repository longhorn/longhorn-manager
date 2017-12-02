package k8s

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	EnvNodeName     = "NODE_NAME"
	EnvPodNamespace = "POD_NAMESPACE"
	EnvPodIP        = "POD_IP"
)

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
