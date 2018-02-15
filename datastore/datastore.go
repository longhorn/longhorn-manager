package datastore

import (
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
	lhlisters "github.com/rancher/longhorn-manager/k8s/pkg/client/listers/longhorn/v1alpha1"
)

type DataStore struct {
	namespace string

	lhClient     lhclientset.Interface
	vLister      lhlisters.VolumeLister
	vStoreSynced cache.InformerSynced
	eLister      lhlisters.ControllerLister
	eStoreSynced cache.InformerSynced
	rLister      lhlisters.ReplicaLister
	rStoreSynced cache.InformerSynced

	kubeClient   clientset.Interface
	pLister      corelisters.PodLister
	pStoreSynced cache.InformerSynced
}

func NewDataStore(
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.ControllerInformer,
	replicaInformer lhinformers.ReplicaInformer,
	lhClient lhclientset.Interface,

	podInformer coreinformers.PodInformer,
	kubeClient clientset.Interface,
	namespace string) *DataStore {

	return &DataStore{
		namespace: namespace,

		lhClient:     lhClient,
		vLister:      volumeInformer.Lister(),
		vStoreSynced: volumeInformer.Informer().HasSynced,
		eLister:      engineInformer.Lister(),
		eStoreSynced: engineInformer.Informer().HasSynced,
		rLister:      replicaInformer.Lister(),
		rStoreSynced: replicaInformer.Informer().HasSynced,

		kubeClient:   kubeClient,
		pLister:      podInformer.Lister(),
		pStoreSynced: podInformer.Informer().HasSynced,
	}
}

func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return !controller.WaitForCacheSync("longhorn datastore", stopCh,
		s.vStoreSynced, s.eStoreSynced, s.rStoreSynced, s.pStoreSynced)
}
