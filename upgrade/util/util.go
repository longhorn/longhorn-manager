package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"
)

func ListShareManagerPods(namespace string, kubeClient *clientset.Clientset) ([]v1.Pod, error) {
	smPodsList, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: labels.Set(types.GetShareManagerComponentLabel()).String(),
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return []v1.Pod{}, nil
		}
		return nil, err
	}
	return smPodsList.Items, nil
}

func ListIMPods(namespace string, kubeClient *clientset.Clientset) ([]v1.Pod, error) {
	imPodsList, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", types.GetLonghornLabelComponentKey(), types.LonghornLabelInstanceManager),
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	if imPodsList != nil {
		return imPodsList.Items, nil
	}
	return []v1.Pod{}, nil
}

func MergeStringMaps(baseMap, overwriteMap map[string]string) map[string]string {
	result := map[string]string{}
	for k, v := range baseMap {
		result[k] = v
	}
	for k, v := range overwriteMap {
		result[k] = v
	}
	return result
}
