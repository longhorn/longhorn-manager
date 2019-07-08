package datastore

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appsinformers_v1beta2 "k8s.io/client-go/informers/apps/v1beta2"
	batchinformers_v1beta1 "k8s.io/client-go/informers/batch/v1beta1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	appslisters_v1beta2 "k8s.io/client-go/listers/apps/v1beta2"
	batchlisters_v1beta1 "k8s.io/client-go/listers/batch/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1alpha1"
)

var (
	SkipListerCheck = false
)

type DataStore struct {
	namespace string

	lhClient      lhclientset.Interface
	vLister       lhlisters.VolumeLister
	vStoreSynced  cache.InformerSynced
	eLister       lhlisters.EngineLister
	eStoreSynced  cache.InformerSynced
	rLister       lhlisters.ReplicaLister
	rStoreSynced  cache.InformerSynced
	iLister       lhlisters.EngineImageLister
	iStoreSynced  cache.InformerSynced
	nLister       lhlisters.NodeLister
	nStoreSynced  cache.InformerSynced
	sLister       lhlisters.SettingLister
	sStoreSynced  cache.InformerSynced
	imLister      lhlisters.InstanceManagerLister
	imStoreSynced cache.InformerSynced

	kubeClient     clientset.Interface
	pLister        corelisters.PodLister
	pStoreSynced   cache.InformerSynced
	cjLister       batchlisters_v1beta1.CronJobLister
	cjStoreSynced  cache.InformerSynced
	dsLister       appslisters_v1beta2.DaemonSetLister
	dsStoreSynced  cache.InformerSynced
	pvLister       corelisters.PersistentVolumeLister
	pvStoreSynced  cache.InformerSynced
	pvcLister      corelisters.PersistentVolumeClaimLister
	pvcStoreSynced cache.InformerSynced
}

func NewDataStore(
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	engineImageInformer lhinformers.EngineImageInformer,
	nodeInformer lhinformers.NodeInformer,
	settingInformer lhinformers.SettingInformer,
	imInformer lhinformers.InstanceManagerInformer,
	lhClient lhclientset.Interface,

	podInformer coreinformers.PodInformer,
	cronJobInformer batchinformers_v1beta1.CronJobInformer,
	daemonSetInformer appsinformers_v1beta2.DaemonSetInformer,
	persistentVolumeInformer coreinformers.PersistentVolumeInformer,
	persistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,

	kubeClient clientset.Interface,
	namespace string) *DataStore {

	return &DataStore{
		namespace: namespace,

		lhClient:      lhClient,
		vLister:       volumeInformer.Lister(),
		vStoreSynced:  volumeInformer.Informer().HasSynced,
		eLister:       engineInformer.Lister(),
		eStoreSynced:  engineInformer.Informer().HasSynced,
		rLister:       replicaInformer.Lister(),
		rStoreSynced:  replicaInformer.Informer().HasSynced,
		iLister:       engineImageInformer.Lister(),
		iStoreSynced:  engineImageInformer.Informer().HasSynced,
		nLister:       nodeInformer.Lister(),
		nStoreSynced:  nodeInformer.Informer().HasSynced,
		sLister:       settingInformer.Lister(),
		sStoreSynced:  settingInformer.Informer().HasSynced,
		imLister:      imInformer.Lister(),
		imStoreSynced: imInformer.Informer().HasSynced,

		kubeClient:     kubeClient,
		pLister:        podInformer.Lister(),
		pStoreSynced:   podInformer.Informer().HasSynced,
		cjLister:       cronJobInformer.Lister(),
		cjStoreSynced:  cronJobInformer.Informer().HasSynced,
		dsLister:       daemonSetInformer.Lister(),
		dsStoreSynced:  daemonSetInformer.Informer().HasSynced,
		pvLister:       persistentVolumeInformer.Lister(),
		pvStoreSynced:  persistentVolumeInformer.Informer().HasSynced,
		pvcLister:      persistentVolumeClaimInformer.Lister(),
		pvcStoreSynced: persistentVolumeClaimInformer.Informer().HasSynced,
	}
}

func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return controller.WaitForCacheSync("longhorn datastore", stopCh,
		s.vStoreSynced, s.eStoreSynced, s.rStoreSynced,
		s.iStoreSynced, s.nStoreSynced, s.sStoreSynced,
		s.pStoreSynced, s.cjStoreSynced, s.dsStoreSynced,
		s.pvStoreSynced, s.pvcStoreSynced, s.imStoreSynced)
}

func ErrorIsNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

func ErrorIsConflict(err error) bool {
	return apierrors.IsConflict(err)
}
