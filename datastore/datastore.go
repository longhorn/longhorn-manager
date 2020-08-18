package datastore

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	batchinformers_v1beta1 "k8s.io/client-go/informers/batch/v1beta1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	clientset "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters_v1beta1 "k8s.io/client-go/listers/batch/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1beta1"
)

var (
	// SkipListerCheck bypass the created longhorn resource validation
	SkipListerCheck = false
)

// DataStore object
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
	dsLister       appslisters.DaemonSetLister
	dsStoreSynced  cache.InformerSynced
	dpLister       appslisters.DeploymentLister
	dpStoreSynced  cache.InformerSynced
	pvLister       corelisters.PersistentVolumeLister
	pvStoreSynced  cache.InformerSynced
	pvcLister      corelisters.PersistentVolumeClaimLister
	pvcStoreSynced cache.InformerSynced
	knLister       corelisters.NodeLister
	knStoreSynced  cache.InformerSynced
	pcLister       schedulinglisters.PriorityClassLister
	pcStoreSynced  cache.InformerSynced
}

// NewDataStore creates new DataStore object
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
	daemonSetInformer appsinformers.DaemonSetInformer,
	deploymentInformer appsinformers.DeploymentInformer,
	persistentVolumeInformer coreinformers.PersistentVolumeInformer,
	persistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,
	kubeNodeInformer coreinformers.NodeInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,

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
		dpLister:       deploymentInformer.Lister(),
		dpStoreSynced:  deploymentInformer.Informer().HasSynced,
		pvLister:       persistentVolumeInformer.Lister(),
		pvStoreSynced:  persistentVolumeInformer.Informer().HasSynced,
		pvcLister:      persistentVolumeClaimInformer.Lister(),
		pvcStoreSynced: persistentVolumeClaimInformer.Informer().HasSynced,
		knLister:       kubeNodeInformer.Lister(),
		knStoreSynced:  kubeNodeInformer.Informer().HasSynced,
		pcLister:       priorityClassInformer.Lister(),
		pcStoreSynced:  priorityClassInformer.Informer().HasSynced,
	}
}

// Sync returns WaitForCacheSync for longhorn datastore
func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return controller.WaitForCacheSync("longhorn datastore", stopCh,
		s.vStoreSynced, s.eStoreSynced, s.rStoreSynced,
		s.iStoreSynced, s.nStoreSynced, s.sStoreSynced,
		s.pStoreSynced, s.cjStoreSynced, s.dsStoreSynced,
		s.pvStoreSynced, s.pvcStoreSynced, s.imStoreSynced,
		s.dpStoreSynced, s.knStoreSynced, s.pcStoreSynced)
}

// ErrorIsNotFound checks if given error match
// metav1.StatusReasonNotFound
func ErrorIsNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

// ErrorIsConflict checks if given error match
// metav1.StatusReasonConflict
func ErrorIsConflict(err error) bool {
	return apierrors.IsConflict(err)
}
