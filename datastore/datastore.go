package datastore

import (
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clientset "k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters_v1 "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	storagelisters_v1 "k8s.io/client-go/listers/storage/v1"

	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1beta2"
)

var (
	// SkipListerCheck bypass the created longhorn resource validation
	SkipListerCheck = false

	// SystemBackupTimeout is the timeout for system backup
	SystemBackupTimeout = time.Hour

	// SystemRestoreTimeout is the timeout for system restore
	SystemRestoreTimeout = 24 * time.Hour

	// VolumeBackupTimeout is the timeout for volume backups
	VolumeBackupTimeout = 24 * time.Hour
)

// DataStore object
type DataStore struct {
	namespace string

	cacheSyncs []cache.InformerSynced

	lhClient                       lhclientset.Interface
	volumeLister                   lhlisters.VolumeLister
	VolumeInformer                 cache.SharedInformer
	engineLister                   lhlisters.EngineLister
	EngineInformer                 cache.SharedInformer
	replicaLister                  lhlisters.ReplicaLister
	ReplicaInformer                cache.SharedInformer
	engineImageLister              lhlisters.EngineImageLister
	EngineImageInformer            cache.SharedInformer
	nodeLister                     lhlisters.NodeLister
	NodeInformer                   cache.SharedInformer
	settingLister                  lhlisters.SettingLister
	SettingInformer                cache.SharedInformer
	instanceManagerLister          lhlisters.InstanceManagerLister
	InstanceManagerInformer        cache.SharedInformer
	shareManagerLister             lhlisters.ShareManagerLister
	ShareManagerInformer           cache.SharedInformer
	backingImageLister             lhlisters.BackingImageLister
	BackingImageInformer           cache.SharedInformer
	backingImageManagerLister      lhlisters.BackingImageManagerLister
	BackingImageManagerInformer    cache.SharedInformer
	backingImageDataSourceLister   lhlisters.BackingImageDataSourceLister
	BackingImageDataSourceInformer cache.SharedInformer
	backupBackingImageLister       lhlisters.BackupBackingImageLister
	BackupBackingImageInformer     cache.SharedInformer
	backupTargetLister             lhlisters.BackupTargetLister
	BackupTargetInformer           cache.SharedInformer
	backupVolumeLister             lhlisters.BackupVolumeLister
	BackupVolumeInformer           cache.SharedInformer
	backupLister                   lhlisters.BackupLister
	BackupInformer                 cache.SharedInformer
	recurringJobLister             lhlisters.RecurringJobLister
	RecurringJobInformer           cache.SharedInformer
	orphanLister                   lhlisters.OrphanLister
	OrphanInformer                 cache.SharedInformer
	snapshotLister                 lhlisters.SnapshotLister
	SnapshotInformer               cache.SharedInformer
	supportBundleLister            lhlisters.SupportBundleLister
	SupportBundleInformer          cache.SharedInformer
	systemBackupLister             lhlisters.SystemBackupLister
	SystemBackupInformer           cache.SharedInformer
	systemRestoreLister            lhlisters.SystemRestoreLister
	SystemRestoreInformer          cache.SharedInformer
	lhVolumeAttachmentLister       lhlisters.VolumeAttachmentLister
	LHVolumeAttachmentInformer     cache.SharedInformer

	kubeClient                    clientset.Interface
	podLister                     corelisters.PodLister
	PodInformer                   cache.SharedInformer
	cronJobLister                 batchlisters_v1.CronJobLister
	CronJobInformer               cache.SharedInformer
	daemonSetLister               appslisters.DaemonSetLister
	DaemonSetInformer             cache.SharedInformer
	deploymentLister              appslisters.DeploymentLister
	DeploymentInformer            cache.SharedInformer
	persistentVolumeLister        corelisters.PersistentVolumeLister
	PersistentVolumeInformer      cache.SharedInformer
	persistentVolumeClaimLister   corelisters.PersistentVolumeClaimLister
	PersistentVolumeClaimInformer cache.SharedInformer
	volumeAttachmentLister        storagelisters_v1.VolumeAttachmentLister
	VolumeAttachmentInformer      cache.SharedInformer
	configMapLister               corelisters.ConfigMapLister
	ConfigMapInformer             cache.SharedInformer
	secretLister                  corelisters.SecretLister
	SecretInformer                cache.SharedInformer
	kubeNodeLister                corelisters.NodeLister
	KubeNodeInformer              cache.SharedInformer
	priorityClassLister           schedulinglisters.PriorityClassLister
	PriorityClassInformer         cache.SharedInformer
	csiDriverLister               storagelisters_v1.CSIDriverLister
	CSIDriverInformer             cache.SharedInformer
	storageclassLister            storagelisters_v1.StorageClassLister
	StorageClassInformer          cache.SharedInformer
	podDisruptionBudgetLister     policylisters.PodDisruptionBudgetLister
	PodDisruptionBudgetInformer   cache.SharedInformer
	serviceLister                 corelisters.ServiceLister
	ServiceInformer               cache.SharedInformer

	extensionsClient apiextensionsclientset.Interface
}

// NewDataStore creates new DataStore object
func NewDataStore(
	lhInformerFactory lhinformers.SharedInformerFactory,
	lhClient lhclientset.Interface,
	kubeInformerFactory informers.SharedInformerFactory,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclientset.Interface,
	namespace string) *DataStore {

	cacheSyncs := []cache.InformerSynced{}

	replicaInformer := lhInformerFactory.Longhorn().V1beta2().Replicas()
	cacheSyncs = append(cacheSyncs, replicaInformer.Informer().HasSynced)
	engineInformer := lhInformerFactory.Longhorn().V1beta2().Engines()
	cacheSyncs = append(cacheSyncs, engineInformer.Informer().HasSynced)
	volumeInformer := lhInformerFactory.Longhorn().V1beta2().Volumes()
	cacheSyncs = append(cacheSyncs, volumeInformer.Informer().HasSynced)
	engineImageInformer := lhInformerFactory.Longhorn().V1beta2().EngineImages()
	cacheSyncs = append(cacheSyncs, engineImageInformer.Informer().HasSynced)
	nodeInformer := lhInformerFactory.Longhorn().V1beta2().Nodes()
	cacheSyncs = append(cacheSyncs, nodeInformer.Informer().HasSynced)
	settingInformer := lhInformerFactory.Longhorn().V1beta2().Settings()
	cacheSyncs = append(cacheSyncs, settingInformer.Informer().HasSynced)
	instanceManagerInformer := lhInformerFactory.Longhorn().V1beta2().InstanceManagers()
	cacheSyncs = append(cacheSyncs, instanceManagerInformer.Informer().HasSynced)
	shareManagerInformer := lhInformerFactory.Longhorn().V1beta2().ShareManagers()
	cacheSyncs = append(cacheSyncs, shareManagerInformer.Informer().HasSynced)
	backingImageInformer := lhInformerFactory.Longhorn().V1beta2().BackingImages()
	cacheSyncs = append(cacheSyncs, backingImageInformer.Informer().HasSynced)
	backingImageManagerInformer := lhInformerFactory.Longhorn().V1beta2().BackingImageManagers()
	cacheSyncs = append(cacheSyncs, backingImageManagerInformer.Informer().HasSynced)
	backingImageDataSourceInformer := lhInformerFactory.Longhorn().V1beta2().BackingImageDataSources()
	cacheSyncs = append(cacheSyncs, backingImageDataSourceInformer.Informer().HasSynced)
	backupBackingImageInformer := lhInformerFactory.Longhorn().V1beta2().BackupBackingImages()
	cacheSyncs = append(cacheSyncs, backupBackingImageInformer.Informer().HasSynced)
	backupTargetInformer := lhInformerFactory.Longhorn().V1beta2().BackupTargets()
	cacheSyncs = append(cacheSyncs, backupTargetInformer.Informer().HasSynced)
	backupVolumeInformer := lhInformerFactory.Longhorn().V1beta2().BackupVolumes()
	cacheSyncs = append(cacheSyncs, backupVolumeInformer.Informer().HasSynced)
	backupInformer := lhInformerFactory.Longhorn().V1beta2().Backups()
	cacheSyncs = append(cacheSyncs, backupInformer.Informer().HasSynced)
	recurringJobInformer := lhInformerFactory.Longhorn().V1beta2().RecurringJobs()
	cacheSyncs = append(cacheSyncs, recurringJobInformer.Informer().HasSynced)
	orphanInformer := lhInformerFactory.Longhorn().V1beta2().Orphans()
	cacheSyncs = append(cacheSyncs, orphanInformer.Informer().HasSynced)
	snapshotInformer := lhInformerFactory.Longhorn().V1beta2().Snapshots()
	cacheSyncs = append(cacheSyncs, snapshotInformer.Informer().HasSynced)
	supportBundleInformer := lhInformerFactory.Longhorn().V1beta2().SupportBundles()
	cacheSyncs = append(cacheSyncs, supportBundleInformer.Informer().HasSynced)
	systemBackupInformer := lhInformerFactory.Longhorn().V1beta2().SystemBackups()
	cacheSyncs = append(cacheSyncs, systemBackupInformer.Informer().HasSynced)
	systemRestoreInformer := lhInformerFactory.Longhorn().V1beta2().SystemRestores()
	cacheSyncs = append(cacheSyncs, systemRestoreInformer.Informer().HasSynced)
	lhVolumeAttachmentInformer := lhInformerFactory.Longhorn().V1beta2().VolumeAttachments()
	cacheSyncs = append(cacheSyncs, lhVolumeAttachmentInformer.Informer().HasSynced)

	podInformer := kubeInformerFactory.Core().V1().Pods()
	cacheSyncs = append(cacheSyncs, podInformer.Informer().HasSynced)
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	cacheSyncs = append(cacheSyncs, kubeNodeInformer.Informer().HasSynced)
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	cacheSyncs = append(cacheSyncs, persistentVolumeInformer.Informer().HasSynced)
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	cacheSyncs = append(cacheSyncs, persistentVolumeClaimInformer.Informer().HasSynced)
	volumeAttachmentInformer := kubeInformerFactory.Storage().V1().VolumeAttachments()
	cacheSyncs = append(cacheSyncs, volumeAttachmentInformer.Informer().HasSynced)
	configMapInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	cacheSyncs = append(cacheSyncs, configMapInformer.Informer().HasSynced)
	secretInformer := kubeInformerFactory.Core().V1().Secrets()
	cacheSyncs = append(cacheSyncs, secretInformer.Informer().HasSynced)
	cronJobInformer := kubeInformerFactory.Batch().V1().CronJobs()
	cacheSyncs = append(cacheSyncs, cronJobInformer.Informer().HasSynced)
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	cacheSyncs = append(cacheSyncs, daemonSetInformer.Informer().HasSynced)
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	cacheSyncs = append(cacheSyncs, deploymentInformer.Informer().HasSynced)
	priorityClassInformer := kubeInformerFactory.Scheduling().V1().PriorityClasses()
	cacheSyncs = append(cacheSyncs, priorityClassInformer.Informer().HasSynced)
	csiDriverInformer := kubeInformerFactory.Storage().V1().CSIDrivers()
	cacheSyncs = append(cacheSyncs, csiDriverInformer.Informer().HasSynced)
	storageclassInformer := kubeInformerFactory.Storage().V1().StorageClasses()
	cacheSyncs = append(cacheSyncs, storageclassInformer.Informer().HasSynced)
	podDisruptionBudgetInformer := kubeInformerFactory.Policy().V1().PodDisruptionBudgets()
	cacheSyncs = append(cacheSyncs, podDisruptionBudgetInformer.Informer().HasSynced)
	serviceInformer := kubeInformerFactory.Core().V1().Services()
	cacheSyncs = append(cacheSyncs, serviceInformer.Informer().HasSynced)

	return &DataStore{
		namespace: namespace,

		cacheSyncs: cacheSyncs,

		lhClient:                       lhClient,
		volumeLister:                   volumeInformer.Lister(),
		VolumeInformer:                 volumeInformer.Informer(),
		engineLister:                   engineInformer.Lister(),
		EngineInformer:                 engineInformer.Informer(),
		replicaLister:                  replicaInformer.Lister(),
		ReplicaInformer:                replicaInformer.Informer(),
		engineImageLister:              engineImageInformer.Lister(),
		EngineImageInformer:            engineImageInformer.Informer(),
		nodeLister:                     nodeInformer.Lister(),
		NodeInformer:                   nodeInformer.Informer(),
		settingLister:                  settingInformer.Lister(),
		SettingInformer:                settingInformer.Informer(),
		instanceManagerLister:          instanceManagerInformer.Lister(),
		InstanceManagerInformer:        instanceManagerInformer.Informer(),
		shareManagerLister:             shareManagerInformer.Lister(),
		ShareManagerInformer:           shareManagerInformer.Informer(),
		backingImageLister:             backingImageInformer.Lister(),
		BackingImageInformer:           backingImageInformer.Informer(),
		backingImageManagerLister:      backingImageManagerInformer.Lister(),
		BackingImageManagerInformer:    backingImageManagerInformer.Informer(),
		backingImageDataSourceLister:   backingImageDataSourceInformer.Lister(),
		BackingImageDataSourceInformer: backingImageDataSourceInformer.Informer(),
		backupBackingImageLister:       backupBackingImageInformer.Lister(),
		BackupBackingImageInformer:     backupBackingImageInformer.Informer(),
		backupTargetLister:             backupTargetInformer.Lister(),
		BackupTargetInformer:           backupTargetInformer.Informer(),
		backupVolumeLister:             backupVolumeInformer.Lister(),
		BackupVolumeInformer:           backupVolumeInformer.Informer(),
		backupLister:                   backupInformer.Lister(),
		BackupInformer:                 backupInformer.Informer(),
		recurringJobLister:             recurringJobInformer.Lister(),
		RecurringJobInformer:           recurringJobInformer.Informer(),
		orphanLister:                   orphanInformer.Lister(),
		OrphanInformer:                 orphanInformer.Informer(),
		snapshotLister:                 snapshotInformer.Lister(),
		SnapshotInformer:               snapshotInformer.Informer(),
		supportBundleLister:            supportBundleInformer.Lister(),
		SupportBundleInformer:          supportBundleInformer.Informer(),
		systemBackupLister:             systemBackupInformer.Lister(),
		SystemBackupInformer:           systemBackupInformer.Informer(),
		systemRestoreLister:            systemRestoreInformer.Lister(),
		SystemRestoreInformer:          systemRestoreInformer.Informer(),
		lhVolumeAttachmentLister:       lhVolumeAttachmentInformer.Lister(),
		LHVolumeAttachmentInformer:     lhVolumeAttachmentInformer.Informer(),

		kubeClient:                    kubeClient,
		podLister:                     podInformer.Lister(),
		PodInformer:                   podInformer.Informer(),
		cronJobLister:                 cronJobInformer.Lister(),
		CronJobInformer:               cronJobInformer.Informer(),
		daemonSetLister:               daemonSetInformer.Lister(),
		DaemonSetInformer:             daemonSetInformer.Informer(),
		deploymentLister:              deploymentInformer.Lister(),
		DeploymentInformer:            deploymentInformer.Informer(),
		persistentVolumeLister:        persistentVolumeInformer.Lister(),
		PersistentVolumeInformer:      persistentVolumeInformer.Informer(),
		persistentVolumeClaimLister:   persistentVolumeClaimInformer.Lister(),
		PersistentVolumeClaimInformer: persistentVolumeClaimInformer.Informer(),
		volumeAttachmentLister:        volumeAttachmentInformer.Lister(),
		VolumeAttachmentInformer:      volumeAttachmentInformer.Informer(),
		configMapLister:               configMapInformer.Lister(),
		ConfigMapInformer:             configMapInformer.Informer(),
		secretLister:                  secretInformer.Lister(),
		SecretInformer:                secretInformer.Informer(),
		kubeNodeLister:                kubeNodeInformer.Lister(),
		KubeNodeInformer:              kubeNodeInformer.Informer(),
		priorityClassLister:           priorityClassInformer.Lister(),
		PriorityClassInformer:         priorityClassInformer.Informer(),
		csiDriverLister:               csiDriverInformer.Lister(),
		CSIDriverInformer:             csiDriverInformer.Informer(),
		storageclassLister:            storageclassInformer.Lister(),
		StorageClassInformer:          storageclassInformer.Informer(),
		podDisruptionBudgetLister:     podDisruptionBudgetInformer.Lister(),
		PodDisruptionBudgetInformer:   podDisruptionBudgetInformer.Informer(),
		serviceLister:                 serviceInformer.Lister(),
		ServiceInformer:               serviceInformer.Informer(),

		extensionsClient: extensionsClient,
	}
}

// Sync returns WaitForCacheSync for Longhorn DataStore
func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return cache.WaitForNamedCacheSync("longhorn datastore", stopCh, s.cacheSyncs...)
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
