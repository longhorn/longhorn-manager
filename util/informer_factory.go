package util

import (
	"time"

	"k8s.io/client-go/informers"

	corev1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

type InformerFactories struct {
	KubeInformerFactory                  informers.SharedInformerFactory
	KubeNamespaceFilteredInformerFactory informers.SharedInformerFactory
	LhInformerFactory                    lhinformers.SharedInformerFactory
}

func NewInformerFactories(namespace string, kubeClient clientset.Interface, lhClient versioned.Interface, resyncPeriod time.Duration) *InformerFactories {
	kubeInformerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, resyncPeriod,
		informers.WithTransform(kubeResourceTransform(namespace)),
	)
	kubeNamespaceFilteredInformerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, resyncPeriod, informers.WithNamespace(namespace))
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, resyncPeriod)

	return &InformerFactories{
		KubeInformerFactory:                  kubeInformerFactory,
		KubeNamespaceFilteredInformerFactory: kubeNamespaceFilteredInformerFactory,
		LhInformerFactory:                    lhInformerFactory,
	}
}

func (f *InformerFactories) Start(stopCh <-chan struct{}) {
	go f.KubeInformerFactory.Start(stopCh)
	go f.KubeNamespaceFilteredInformerFactory.Start(stopCh)
	go f.LhInformerFactory.Start(stopCh)
}

// kubeResourceTransform returns a transform function that strips unnecessary fields
// from Kubernetes Pod objects cached by the cluster-wide KubeInformerFactory.
//
// Only Pod objects are transformed because they dominate informer memory usage
// in large clusters. Other resources (PV, PVC, StorageClass, Lease, etc.) are
// left intact to preserve ManagedFields for safe Update operations and future
// server-side apply compatibility.
func kubeResourceTransform(namespace string) func(interface{}) (interface{}, error) {
	return func(obj interface{}) (interface{}, error) {
		if pod, ok := obj.(*corev1.Pod); ok {
			return podTransform(pod, namespace), nil
		}
		return obj, nil
	}
}

// podTransform strips unnecessary fields from Pod objects to reduce informer cache memory.
//
// Pods in the provided Longhorn namespace (typically the Longhorn system namespace)
// are kept intact because controllers may update them.
// Pods outside the provided Longhorn namespace are never updated by longhorn-manager
// (only deleted), so ManagedFields and other fields can be safely stripped.
//
// For Pods outside the provided Longhorn namespace with PVC/Ephemeral volumes,
// all fields are preserved (except ManagedFields) because multiple controllers
// (KubernetesPVController, KubernetesPodController) access various fields.
// Stripping individual fields is fragile and hard to maintain as field access
// patterns change.
//
// For Pods outside the provided Longhorn namespace without PVC/Ephemeral volumes,
// Spec and Status are fully cleared since these Pods are never matched by
// volume-related lookups.
func podTransform(pod *corev1.Pod, namespace string) *corev1.Pod {
	if pod.Namespace == namespace {
		return pod
	}

	pod.ManagedFields = nil

	hasPVC := false
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].PersistentVolumeClaim != nil || pod.Spec.Volumes[i].Ephemeral != nil {
			hasPVC = true
			break
		}
	}

	if !hasPVC {
		pod.Spec = corev1.PodSpec{}
		pod.Status = corev1.PodStatus{}
		pod.Annotations = nil
		pod.Labels = nil
		pod.OwnerReferences = nil
		pod.Finalizers = nil
	}

	return pod
}
