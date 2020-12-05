package datastore

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	batchinformers_v1beta1 "k8s.io/client-go/informers/batch/v1beta1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	policyinformers "k8s.io/client-go/informers/policy/v1beta1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	storageinformers_v1 "k8s.io/client-go/informers/storage/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters_v1beta1 "k8s.io/client-go/listers/batch/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1beta1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	storagelisters_v1 "k8s.io/client-go/listers/storage/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1beta1"
	"k8s.io/client-go/tools/cache"

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
	smLister      lhlisters.ShareManagerLister
	smStoreSynced cache.InformerSynced

	kubeClient         clientset.Interface
	pLister            corelisters.PodLister
	pStoreSynced       cache.InformerSynced
	cjLister           batchlisters_v1beta1.CronJobLister
	cjStoreSynced      cache.InformerSynced
	dsLister           appslisters.DaemonSetLister
	dsStoreSynced      cache.InformerSynced
	dpLister           appslisters.DeploymentLister
	dpStoreSynced      cache.InformerSynced
	pvLister           corelisters.PersistentVolumeLister
	pvStoreSynced      cache.InformerSynced
	pvcLister          corelisters.PersistentVolumeClaimLister
	pvcStoreSynced     cache.InformerSynced
	cfmLister          corelisters.ConfigMapLister
	cfmStoreSynced     cache.InformerSynced
	knLister           corelisters.NodeLister
	knStoreSynced      cache.InformerSynced
	pcLister           schedulinglisters.PriorityClassLister
	pcStoreSynced      cache.InformerSynced
	csiDriverLister    storagelisters.CSIDriverLister
	csiDriverSynced    cache.InformerSynced
	storageclassLister storagelisters_v1.StorageClassLister
	storageclassSynced cache.InformerSynced
	pdbLister          policylisters.PodDisruptionBudgetLister
	pdbStoreSynced     cache.InformerSynced
	svLister           corelisters.ServiceLister
	svStoreSynced      cache.InformerSynced
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
	smInformer lhinformers.ShareManagerInformer,
	lhClient lhclientset.Interface,

	podInformer coreinformers.PodInformer,
	cronJobInformer batchinformers_v1beta1.CronJobInformer,
	daemonSetInformer appsinformers.DaemonSetInformer,
	deploymentInformer appsinformers.DeploymentInformer,
	persistentVolumeInformer coreinformers.PersistentVolumeInformer,
	persistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	kubeNodeInformer coreinformers.NodeInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,
	csiDriverInformer storageinformers.CSIDriverInformer,
	storageclassInformer storageinformers_v1.StorageClassInformer,
	pdbInformer policyinformers.PodDisruptionBudgetInformer,
	serviceInformer coreinformers.ServiceInformer,

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
		smLister:      smInformer.Lister(),
		smStoreSynced: smInformer.Informer().HasSynced,

		kubeClient:         kubeClient,
		pLister:            podInformer.Lister(),
		pStoreSynced:       podInformer.Informer().HasSynced,
		cjLister:           cronJobInformer.Lister(),
		cjStoreSynced:      cronJobInformer.Informer().HasSynced,
		dsLister:           daemonSetInformer.Lister(),
		dsStoreSynced:      daemonSetInformer.Informer().HasSynced,
		dpLister:           deploymentInformer.Lister(),
		dpStoreSynced:      deploymentInformer.Informer().HasSynced,
		pvLister:           persistentVolumeInformer.Lister(),
		pvStoreSynced:      persistentVolumeInformer.Informer().HasSynced,
		pvcLister:          persistentVolumeClaimInformer.Lister(),
		pvcStoreSynced:     persistentVolumeClaimInformer.Informer().HasSynced,
		cfmLister:          configMapInformer.Lister(),
		cfmStoreSynced:     configMapInformer.Informer().HasSynced,
		knLister:           kubeNodeInformer.Lister(),
		knStoreSynced:      kubeNodeInformer.Informer().HasSynced,
		pcLister:           priorityClassInformer.Lister(),
		pcStoreSynced:      priorityClassInformer.Informer().HasSynced,
		csiDriverLister:    csiDriverInformer.Lister(),
		csiDriverSynced:    csiDriverInformer.Informer().HasSynced,
		storageclassLister: storageclassInformer.Lister(),
		storageclassSynced: storageclassInformer.Informer().HasSynced,
		pdbLister:          pdbInformer.Lister(),
		pdbStoreSynced:     pdbInformer.Informer().HasSynced,
		svLister:           serviceInformer.Lister(),
		svStoreSynced:      serviceInformer.Informer().HasSynced,
	}
}

// Sync returns WaitForCacheSync for Longhorn DataStore
func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return cache.WaitForNamedCacheSync("longhorn datastore", stopCh,
		s.vStoreSynced, s.eStoreSynced, s.rStoreSynced,
		s.iStoreSynced, s.nStoreSynced, s.sStoreSynced,
		s.pStoreSynced, s.cjStoreSynced, s.dsStoreSynced,
		s.pvStoreSynced, s.pvcStoreSynced, s.cfmStoreSynced,
		s.imStoreSynced, s.dpStoreSynced, s.knStoreSynced,
		s.pcStoreSynced, s.csiDriverSynced, s.storageclassSynced,
		s.pdbStoreSynced, s.smStoreSynced, s.svStoreSynced)
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
