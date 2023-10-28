package api

import (
	"fmt"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type Empty struct {
	client.Resource
}

type Volume struct {
	client.Resource

	Name                             string                                 `json:"name"`
	Size                             string                                 `json:"size"`
	Frontend                         longhorn.VolumeFrontend                `json:"frontend"`
	DisableFrontend                  bool                                   `json:"disableFrontend"`
	FromBackup                       string                                 `json:"fromBackup"`
	RestoreVolumeRecurringJob        longhorn.RestoreVolumeRecurringJobType `json:"restoreVolumeRecurringJob"`
	DataSource                       longhorn.VolumeDataSource              `json:"dataSource"`
	DataLocality                     longhorn.DataLocality                  `json:"dataLocality"`
	StaleReplicaTimeout              int                                    `json:"staleReplicaTimeout"`
	State                            longhorn.VolumeState                   `json:"state"`
	Robustness                       longhorn.VolumeRobustness              `json:"robustness"`
	Image                            string                                 `json:"image"`
	CurrentImage                     string                                 `json:"currentImage"`
	BackingImage                     string                                 `json:"backingImage"`
	Created                          string                                 `json:"created"`
	LastBackup                       string                                 `json:"lastBackup"`
	LastBackupAt                     string                                 `json:"lastBackupAt"`
	LastAttachedBy                   string                                 `json:"lastAttachedBy"`
	Standby                          bool                                   `json:"standby"`
	RestoreRequired                  bool                                   `json:"restoreRequired"`
	RestoreInitiated                 bool                                   `json:"restoreInitiated"`
	RevisionCounterDisabled          bool                                   `json:"revisionCounterDisabled"`
	SnapshotDataIntegrity            longhorn.SnapshotDataIntegrity         `json:"snapshotDataIntegrity"`
	UnmapMarkSnapChainRemoved        longhorn.UnmapMarkSnapChainRemoved     `json:"unmapMarkSnapChainRemoved"`
	BackupCompressionMethod          longhorn.BackupCompressionMethod       `json:"backupCompressionMethod"`
	ReplicaSoftAntiAffinity          longhorn.ReplicaSoftAntiAffinity       `json:"replicaSoftAntiAffinity"`
	ReplicaZoneSoftAntiAffinity      longhorn.ReplicaZoneSoftAntiAffinity   `json:"replicaZoneSoftAntiAffinity"`
	ReplicaDiskSoftAntiAffinity      longhorn.ReplicaDiskSoftAntiAffinity   `json:"replicaDiskSoftAntiAffinity"`
	BackendStoreDriver               longhorn.BackendStoreDriverType        `json:"backendStoreDriver"`
	OfflineReplicaRebuilding         longhorn.OfflineReplicaRebuilding      `json:"offlineReplicaRebuilding"`
	OfflineReplicaRebuildingRequired bool                                   `json:"offlineReplicaRebuildingRequired"`

	DiskSelector         []string                      `json:"diskSelector"`
	NodeSelector         []string                      `json:"nodeSelector"`
	RecurringJobSelector []longhorn.VolumeRecurringJob `json:"recurringJobSelector"`

	NumberOfReplicas   int                         `json:"numberOfReplicas"`
	ReplicaAutoBalance longhorn.ReplicaAutoBalance `json:"replicaAutoBalance"`

	Conditions       map[string]longhorn.Condition `json:"conditions"`
	KubernetesStatus longhorn.KubernetesStatus     `json:"kubernetesStatus"`
	CloneStatus      longhorn.VolumeCloneStatus    `json:"cloneStatus"`
	Ready            bool                          `json:"ready"`

	AccessMode    longhorn.AccessMode        `json:"accessMode"`
	ShareEndpoint string                     `json:"shareEndpoint"`
	ShareState    longhorn.ShareManagerState `json:"shareState"`

	Migratable bool `json:"migratable"`

	Encrypted bool `json:"encrypted"`

	Replicas         []Replica        `json:"replicas"`
	Controllers      []Controller     `json:"controllers"`
	BackupStatus     []BackupStatus   `json:"backupStatus"`
	RestoreStatus    []RestoreStatus  `json:"restoreStatus"`
	PurgeStatus      []PurgeStatus    `json:"purgeStatus"`
	RebuildStatus    []RebuildStatus  `json:"rebuildStatus"`
	VolumeAttachment VolumeAttachment `json:"volumeAttachment"`
}

// Snapshot struct is used for the snapshot* actions
type Snapshot struct {
	client.Resource
	longhorn.SnapshotInfo
	Checksum string `json:"checksum"`
}

// SnapshotCR struct is used for the snapshotCR* actions
type SnapshotCR struct {
	client.Resource
	Name           string `json:"name"`
	CRCreationTime string `json:"crCreationTime"`
	Volume         string `json:"volume"`
	CreateSnapshot bool   `json:"createSnapshot"`

	Parent       string            `json:"parent"`
	Children     map[string]bool   `json:"children"`
	MarkRemoved  bool              `json:"markRemoved"`
	UserCreated  bool              `json:"userCreated"`
	CreationTime string            `json:"creationTime"`
	Size         int64             `json:"size"`
	Labels       map[string]string `json:"labels"`
	OwnerID      string            `json:"ownerID"`
	Error        string            `json:"error,omitempty"`
	RestoreSize  int64             `json:"restoreSize"`
	ReadyToUse   bool              `json:"readyToUse"`
	Checksum     string            `json:"checksum"`
}

type BackupTarget struct {
	client.Resource
	engineapi.BackupTarget
}

type BackupVolume struct {
	client.Resource

	Name                 string            `json:"name"`
	Size                 string            `json:"size"`
	Labels               map[string]string `json:"labels"`
	Created              string            `json:"created"`
	LastBackupName       string            `json:"lastBackupName"`
	LastBackupAt         string            `json:"lastBackupAt"`
	DataStored           string            `json:"dataStored"`
	Messages             map[string]string `json:"messages"`
	BackingImageName     string            `json:"backingImageName"`
	BackingImageChecksum string            `json:"backingImageChecksum"`
	StorageClassName     string            `json:"storageClassName"`
}

type Backup struct {
	client.Resource

	Name                   string               `json:"name"`
	State                  longhorn.BackupState `json:"state"`
	Progress               int                  `json:"progress"`
	Error                  string               `json:"error"`
	URL                    string               `json:"url"`
	SnapshotName           string               `json:"snapshotName"`
	SnapshotCreated        string               `json:"snapshotCreated"`
	Created                string               `json:"created"`
	Size                   string               `json:"size"`
	Labels                 map[string]string    `json:"labels"`
	Messages               map[string]string    `json:"messages"`
	VolumeName             string               `json:"volumeName"`
	VolumeSize             string               `json:"volumeSize"`
	VolumeCreated          string               `json:"volumeCreated"`
	VolumeBackingImageName string               `json:"volumeBackingImageName"`
	CompressionMethod      string               `json:"compressionMethod"`
}

type Setting struct {
	client.Resource
	Name       string                  `json:"name"`
	Value      string                  `json:"value"`
	Definition types.SettingDefinition `json:"definition"`
}

type Instance struct {
	Name                string `json:"name"`
	NodeID              string `json:"hostId"`
	Address             string `json:"address"`
	Running             bool   `json:"running"`
	Image               string `json:"image"`
	CurrentImage        string `json:"currentImage"`
	InstanceManagerName string `json:"instanceManagerName"`
}

type Controller struct {
	Instance
	Size                             string `json:"size"`
	ActualSize                       string `json:"actualSize"`
	Endpoint                         string `json:"endpoint"`
	LastRestoredBackup               string `json:"lastRestoredBackup"`
	RequestedBackupRestore           string `json:"requestedBackupRestore"`
	IsExpanding                      bool   `json:"isExpanding"`
	LastExpansionError               string `json:"lastExpansionError"`
	LastExpansionFailedAt            string `json:"lastExpansionFailedAt"`
	UnmapMarkSnapChainRemovedEnabled bool   `json:"unmapMarkSnapChainRemovedEnabled"`
}

type Replica struct {
	Instance

	DiskID             string `json:"diskID"`
	DiskPath           string `json:"diskPath"`
	DataPath           string `json:"dataPath"`
	Mode               string `json:"mode"`
	FailedAt           string `json:"failedAt"`
	BackendStoreDriver string `json:"backendStoreDriver"`
}

type Attachment struct {
	AttachmentID   string            `json:"attachmentID"`
	AttachmentType string            `json:"attachmentType"`
	NodeID         string            `json:"nodeID"`
	Parameters     map[string]string `json:"parameters"`
	// Indicate whether this attachment ticket has been satisfied
	Satisfied  bool                 `json:"satisfied"`
	Conditions []longhorn.Condition `json:"conditions"`
}

type VolumeAttachment struct {
	Attachments map[string]Attachment `json:"attachments"`
	Volume      string                `json:"volume"`
}

type EngineImage struct {
	client.Resource

	Name    string `json:"name"`
	Image   string `json:"image"`
	Default bool   `json:"default"`
	longhorn.EngineImageStatus
}

type BackingImage struct {
	client.Resource

	Name             string            `json:"name"`
	UUID             string            `json:"uuid"`
	SourceType       string            `json:"sourceType"`
	Parameters       map[string]string `json:"parameters"`
	ExpectedChecksum string            `json:"expectedChecksum"`

	DiskFileStatusMap map[string]longhorn.BackingImageDiskFileStatus `json:"diskFileStatusMap"`
	Size              int64                                          `json:"size"`
	CurrentChecksum   string                                         `json:"currentChecksum"`

	DeletionTimestamp string `json:"deletionTimestamp"`
}

type BackingImageCleanupInput struct {
	Disks []string `json:"disks"`
}

type AttachInput struct {
	HostID          string `json:"hostId"`
	DisableFrontend bool   `json:"disableFrontend"`
	AttachedBy      string `json:"attachedBy"`
	AttacherType    string `json:"attacherType"`
	AttachmentID    string `json:"attachmentID"`
}

type DetachInput struct {
	AttachmentID string `json:"attachmentID"`
	HostID       string `json:"hostId"`
	ForceDetach  bool   `json:"forceDetach"`
}

type SnapshotInput struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type SnapshotCRInput struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type BackupInput struct {
	Name string `json:"name"`
}

type ReplicaRemoveInput struct {
	Name string `json:"name"`
}

type SalvageInput struct {
	Names []string `json:"names"`
}

type EngineUpgradeInput struct {
	Image string `json:"image"`
}

type UpdateReplicaCountInput struct {
	ReplicaCount int `json:"replicaCount"`
}

type UpdateReplicaAutoBalanceInput struct {
	ReplicaAutoBalance string `json:"replicaAutoBalance"`
}

type UpdateDataLocalityInput struct {
	DataLocality string `json:"dataLocality"`
}

type UpdateAccessModeInput struct {
	AccessMode string `json:"accessMode"`
}

type UpdateSnapshotDataIntegrityInput struct {
	SnapshotDataIntegrity string `json:"snapshotDataIntegrity"`
}

type UpdateOfflineReplicaRebuildingInput struct {
	OfflineReplicaRebuilding string `json:"offlineReplicaRebuilding"`
}

type UpdateBackupCompressionMethodInput struct {
	BackupCompressionMethod string `json:"backupCompressionMethod"`
}

type UpdateUnmapMarkSnapChainRemovedInput struct {
	UnmapMarkSnapChainRemoved string `json:"unmapMarkSnapChainRemoved"`
}

type UpdateReplicaSoftAntiAffinityInput struct {
	ReplicaSoftAntiAffinity string `json:"replicaSoftAntiAffinity"`
}

type UpdateReplicaZoneSoftAntiAffinityInput struct {
	ReplicaZoneSoftAntiAffinity string `json:"replicaZoneSoftAntiAffinity"`
}

type UpdateReplicaDiskSoftAntiAffinityInput struct {
	ReplicaDiskSoftAntiAffinity string `json:"replicaDiskSoftAntiAffinity"`
}

type PVCreateInput struct {
	PVName string `json:"pvName"`
	FSType string `json:"fsType"`

	SecretName      string `json:"secretName"`
	SecretNamespace string `json:"secretNamespace"`

	StorageClassName string `json:"storageClassName"`
}

type PVCCreateInput struct {
	Namespace string `json:"namespace"`
	PVCName   string `json:"pvcName"`
}

type ActivateInput struct {
	Frontend string `json:"frontend"`
}

type ExpandInput struct {
	Size string `json:"size"`
}

type Node struct {
	client.Resource
	Name                      string                        `json:"name"`
	Address                   string                        `json:"address"`
	AllowScheduling           bool                          `json:"allowScheduling"`
	EvictionRequested         bool                          `json:"evictionRequested"`
	Disks                     map[string]DiskInfo           `json:"disks"`
	Conditions                map[string]longhorn.Condition `json:"conditions"`
	Tags                      []string                      `json:"tags"`
	Region                    string                        `json:"region"`
	Zone                      string                        `json:"zone"`
	InstanceManagerCPURequest int                           `json:"instanceManagerCPURequest"`
	AutoEvicting              bool                          `json:"autoEvicting"`
}

type DiskStatus struct {
	Conditions       map[string]longhorn.Condition `json:"conditions"`
	StorageAvailable int64                         `json:"storageAvailable"`
	StorageScheduled int64                         `json:"storageScheduled"`
	StorageMaximum   int64                         `json:"storageMaximum"`
	ScheduledReplica map[string]int64              `json:"scheduledReplica"`
	DiskUUID         string                        `json:"diskUUID"`
}

type DiskInfo struct {
	longhorn.DiskSpec
	// To convert CR status.Conditions from datatype map to slice, replace longhorn.DiskStatus with DiskStatus to keep it use datatype map.
	// Therefore, the UI (RESTful endpoint) does not need to do any changes.
	DiskStatus
}

type DiskUpdateInput struct {
	Disks map[string]longhorn.DiskSpec `json:"disks"`
}

type Event struct {
	client.Resource
	Event     corev1.Event `json:"event"`
	EventType string       `json:"eventType"`
}

type SupportBundle struct {
	client.Resource
	NodeID             string                      `json:"nodeID"`
	State              longhorn.SupportBundleState `json:"state"`
	Name               string                      `json:"name"`
	ErrorMessage       string                      `json:"errorMessage"`
	ProgressPercentage int                         `json:"progressPercentage"`
}

type SupportBundleInitateInput struct {
	IssueURL    string `json:"issueURL"`
	Description string `json:"description"`
}

type SystemBackup struct {
	client.Resource

	Name               string                                        `json:"name"`
	VolumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy `json:"volumeBackupPolicy"`

	Version      string                     `json:"version,omitempty"`
	ManagerImage string                     `json:"managerImage,omitempty"`
	State        longhorn.SystemBackupState `json:"state,omitempty"`
	CreatedAt    string                     `json:"createdAt,omitempty"`
	Error        string                     `json:"error,omitempty"`
}

type SystemBackupInput struct {
	Name               string                                        `json:"name"`
	VolumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy `json:"volumeBackupPolicy"`
}

type SystemRestore struct {
	client.Resource
	Name         string                      `json:"name"`
	SystemBackup string                      `json:"systemBackup"`
	State        longhorn.SystemRestoreState `json:"state,omitempty"`
	CreatedAt    string                      `json:"createdAt,omitempty"`
	Error        string                      `json:"error,omitempty"`
}

type SystemRestoreInput struct {
	Name         string `json:"name"`
	SystemBackup string `json:"systemBackup"`
}

type Tag struct {
	client.Resource
	Name    string `json:"name"`
	TagType string `json:"tagType"`
}

type BackupStatus struct {
	client.Resource
	Name      string `json:"id"`
	Snapshot  string `json:"snapshot"`
	Progress  int    `json:"progress"`
	BackupURL string `json:"backupURL"`
	Error     string `json:"error"`
	State     string `json:"state"`
	Replica   string `json:"replica"`
	Size      string `json:"size"`
}

type RestoreStatus struct {
	client.Resource
	Replica      string `json:"replica"`
	IsRestoring  bool   `json:"isRestoring"`
	LastRestored string `json:"lastRestored"`
	Progress     int    `json:"progress"`
	Error        string `json:"error"`
	Filename     string `json:"filename"`
	State        string `json:"state"`
	BackupURL    string `json:"backupURL"`
}

type PurgeStatus struct {
	client.Resource
	Error     string `json:"error"`
	IsPurging bool   `json:"isPurging"`
	Progress  int    `json:"progress"`
	Replica   string `json:"replica"`
	State     string `json:"state"`
}

type RebuildStatus struct {
	client.Resource
	Error        string `json:"error"`
	IsRebuilding bool   `json:"isRebuilding"`
	Progress     int    `json:"progress"`
	Replica      string `json:"replica"`
	State        string `json:"state"`
	FromReplica  string `json:"fromReplica"`
}

type InstanceManager struct {
	client.Resource
	CurrentState       longhorn.InstanceManagerState `json:"currentState"`
	Image              string                        `json:"image"`
	Name               string                        `json:"name"`
	NodeID             string                        `json:"nodeID"`
	ManagerType        string                        `json:"managerType"`
	BackendStoreDriver string                        `json:"backendStoreDriver"`

	InstanceEngines  map[string]longhorn.InstanceProcess `json:"instanceEngines"`
	InstanceReplicas map[string]longhorn.InstanceProcess `json:"instanceReplicas"`

	// Deprecated
	Instances map[string]longhorn.InstanceProcess `json:"instances"`
}

type RecurringJob struct {
	client.Resource
	longhorn.RecurringJobSpec
}

type Orphan struct {
	client.Resource
	Name string `json:"name"`
	longhorn.OrphanSpec
}

type VolumeRecurringJob struct {
	client.Resource
	longhorn.VolumeRecurringJob
}

type VolumeRecurringJobInput struct {
	longhorn.VolumeRecurringJob
}

type BackupListOutput struct {
	Data []Backup `json:"data"`
	Type string   `json:"type"`
}

type SnapshotListOutput struct {
	Data []Snapshot `json:"data"`
	Type string     `json:"type"`
}

type SnapshotCRListOutput struct {
	Data []SnapshotCR `json:"data"`
	Type string       `json:"type"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("attachInput", AttachInput{})
	schemas.AddType("detachInput", DetachInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("snapshotCRInput", SnapshotCRInput{})
	schemas.AddType("backupTarget", BackupTarget{})
	schemas.AddType("backup", Backup{})
	schemas.AddType("backupInput", BackupInput{})
	schemas.AddType("backupStatus", BackupStatus{})
	schemas.AddType("orphan", Orphan{})
	schemas.AddType("restoreStatus", RestoreStatus{})
	schemas.AddType("purgeStatus", PurgeStatus{})
	schemas.AddType("rebuildStatus", RebuildStatus{})
	schemas.AddType("replicaRemoveInput", ReplicaRemoveInput{})
	schemas.AddType("salvageInput", SalvageInput{})
	schemas.AddType("activateInput", ActivateInput{})
	schemas.AddType("expandInput", ExpandInput{})
	schemas.AddType("engineUpgradeInput", EngineUpgradeInput{})
	schemas.AddType("replica", Replica{})
	schemas.AddType("controller", Controller{})
	schemas.AddType("diskUpdate", longhorn.DiskSpec{})
	schemas.AddType("UpdateReplicaCountInput", UpdateReplicaCountInput{})
	schemas.AddType("UpdateReplicaAutoBalanceInput", UpdateReplicaAutoBalanceInput{})
	schemas.AddType("UpdateDataLocalityInput", UpdateDataLocalityInput{})
	schemas.AddType("UpdateAccessModeInput", UpdateAccessModeInput{})
	schemas.AddType("UpdateSnapshotDataIntegrityInput", UpdateSnapshotDataIntegrityInput{})
	schemas.AddType("UpdateOfflineReplicaRebuildingInput", UpdateOfflineReplicaRebuildingInput{})
	schemas.AddType("UpdateBackupCompressionInput", UpdateBackupCompressionMethodInput{})
	schemas.AddType("UpdateUnmapMarkSnapChainRemovedInput", UpdateUnmapMarkSnapChainRemovedInput{})
	schemas.AddType("UpdateReplicaSoftAntiAffinityInput", UpdateReplicaSoftAntiAffinityInput{})
	schemas.AddType("UpdateReplicaZoneSoftAntiAffinityInput", UpdateReplicaZoneSoftAntiAffinityInput{})
	schemas.AddType("UpdateReplicaDiskSoftAntiAffinityInput", UpdateReplicaDiskSoftAntiAffinityInput{})
	schemas.AddType("workloadStatus", longhorn.WorkloadStatus{})
	schemas.AddType("cloneStatus", longhorn.VolumeCloneStatus{})
	schemas.AddType("empty", Empty{})

	schemas.AddType("volumeRecurringJob", VolumeRecurringJob{})
	schemas.AddType("volumeRecurringJobInput", VolumeRecurringJobInput{})

	schemas.AddType("PVCreateInput", PVCreateInput{})
	schemas.AddType("PVCCreateInput", PVCCreateInput{})

	schemas.AddType("settingDefinition", types.SettingDefinition{})
	// to avoid duplicate name with built-in type condition
	schemas.AddType("volumeCondition", longhorn.Condition{})
	schemas.AddType("nodeCondition", longhorn.Condition{})
	schemas.AddType("diskCondition", longhorn.Condition{})
	schemas.AddType("longhornCondition", longhorn.Condition{})

	schemas.AddType("event", Event{})
	schemas.AddType("supportBundle", SupportBundle{})
	schemas.AddType("supportBundleInitateInput", SupportBundleInitateInput{})

	schemas.AddType("tag", Tag{})

	schemas.AddType("instanceManager", InstanceManager{})
	schemas.AddType("instanceProcess", longhorn.InstanceProcess{})

	schemas.AddType("backingImageDiskFileStatus", longhorn.BackingImageDiskFileStatus{})
	schemas.AddType("backingImageCleanupInput", BackingImageCleanupInput{})

	attachmentSchema(schemas.AddType("attachment", Attachment{}))
	volumeAttachmentSchema(schemas.AddType("volumeAttachment", VolumeAttachment{}))
	volumeSchema(schemas.AddType("volume", Volume{}))
	snapshotSchema(schemas.AddType("snapshot", Snapshot{}))
	snapshotCRSchema(schemas.AddType("snapshotCR", SnapshotCR{}))
	backupVolumeSchema(schemas.AddType("backupVolume", BackupVolume{}))
	settingSchema(schemas.AddType("setting", Setting{}))
	recurringJobSchema(schemas.AddType("recurringJob", RecurringJob{}))
	engineImageSchema(schemas.AddType("engineImage", EngineImage{}))
	backingImageSchema(schemas.AddType("backingImage", BackingImage{}))
	nodeSchema(schemas.AddType("node", Node{}))
	diskSchema(schemas.AddType("diskUpdateInput", DiskUpdateInput{}))
	diskInfoSchema(schemas.AddType("diskInfo", DiskInfo{}))
	kubernetesStatusSchema(schemas.AddType("kubernetesStatus", longhorn.KubernetesStatus{}))
	backupListOutputSchema(schemas.AddType("backupListOutput", BackupListOutput{}))
	snapshotListOutputSchema(schemas.AddType("snapshotListOutput", SnapshotListOutput{}))
	systemBackupSchema(schemas.AddType("systemBackup", SystemBackup{}))
	systemRestoreSchema(schemas.AddType("systemRestore", SystemRestore{}))
	snapshotCRListOutputSchema(schemas.AddType("snapshotCRListOutput", SnapshotCRListOutput{}))

	return schemas
}

func nodeSchema(node *client.Schema) {
	node.CollectionMethods = []string{"GET"}
	node.ResourceMethods = []string{"GET", "PUT"}

	node.ResourceActions = map[string]client.Action{
		"diskUpdate": {
			Input:  "diskUpdateInput",
			Output: "node",
		},
	}

	allowScheduling := node.ResourceFields["allowScheduling"]
	allowScheduling.Required = true
	allowScheduling.Unique = false
	node.ResourceFields["allowScheduling"] = allowScheduling

	evictionRequested := node.ResourceFields["evictionRequested"]
	evictionRequested.Required = true
	evictionRequested.Unique = false
	node.ResourceFields["evictionRequested"] = evictionRequested

	node.ResourceFields["disks"] = client.Field{
		Type:     "map[diskInfo]",
		Nullable: true,
	}

	conditions := node.ResourceFields["conditions"]
	conditions.Type = "map[nodeCondition]"
	node.ResourceFields["conditions"] = conditions

	tags := node.ResourceFields["tags"]
	tags.Create = true
	node.ResourceFields["tags"] = tags
}

func diskSchema(diskUpdateInput *client.Schema) {
	disks := diskUpdateInput.ResourceFields["disks"]
	disks.Type = "array[diskUpdate]"
	diskUpdateInput.ResourceFields["disks"] = disks
}

func diskInfoSchema(diskInfo *client.Schema) {
	conditions := diskInfo.ResourceFields["conditions"]
	conditions.Type = "map[diskCondition]"
	diskInfo.ResourceFields["conditions"] = conditions
}

func engineImageSchema(engineImage *client.Schema) {
	engineImage.CollectionMethods = []string{"GET", "POST"}
	engineImage.ResourceMethods = []string{"GET", "DELETE"}

	image := engineImage.ResourceFields["image"]
	image.Create = true
	image.Required = true
	image.Unique = true
	engineImage.ResourceFields["image"] = image
}

func backingImageSchema(backingImage *client.Schema) {
	backingImage.CollectionMethods = []string{"GET", "POST"}
	backingImage.ResourceMethods = []string{"GET", "DELETE"}

	backingImage.ResourceActions = map[string]client.Action{
		"backingImageCleanup": {
			Input:  "backingImageCleanupInput",
			Output: "backingImage",
		},
		BackingImageUpload: {},
	}

	name := backingImage.ResourceFields["name"]
	name.Required = true
	name.Unique = true
	name.Create = true
	backingImage.ResourceFields["name"] = name

	expectedChecksum := backingImage.ResourceFields["expectedChecksum"]
	expectedChecksum.Create = true
	backingImage.ResourceFields["expectedChecksum"] = expectedChecksum

	sourceType := backingImage.ResourceFields["sourceType"]
	sourceType.Required = true
	sourceType.Create = true
	backingImage.ResourceFields["sourceType"] = sourceType

	parameters := backingImage.ResourceFields["parameters"]
	parameters.Type = "map[string]"
	parameters.Required = true
	parameters.Create = true
	backingImage.ResourceFields["parameters"] = parameters

	diskFileStatusMap := backingImage.ResourceFields["diskFileStatusMap"]
	diskFileStatusMap.Type = "map[backingImageDiskFileStatus]"
	backingImage.ResourceFields["diskFileStatusMap"] = diskFileStatusMap
}

func recurringJobSchema(job *client.Schema) {
	job.CollectionMethods = []string{"GET", "POST"}
	job.ResourceMethods = []string{"GET", "PUT", "DELETE"}

	name := job.ResourceFields["name"]
	name.Required = true
	name.Unique = true
	name.Create = true
	job.ResourceFields["name"] = name

	groups := job.ResourceFields["groups"]
	groups.Type = "array[string]"
	groups.Nullable = true
	job.ResourceFields["groups"] = groups

	cron := job.ResourceFields["cron"]
	cron.Required = true
	cron.Unique = false
	cron.Create = true
	job.ResourceFields["cron"] = cron

	retain := job.ResourceFields["retain"]
	retain.Required = true
	retain.Unique = false
	retain.Create = true
	job.ResourceFields["retain"] = retain

	concurrency := job.ResourceFields["concurrency"]
	concurrency.Required = true
	concurrency.Unique = false
	concurrency.Create = true
	job.ResourceFields["concurrency"] = concurrency

	labels := job.ResourceFields["labels"]
	labels.Type = "map[string]"
	labels.Nullable = true
	job.ResourceFields["labels"] = labels
}

func kubernetesStatusSchema(status *client.Schema) {
	workloadsStatus := status.ResourceFields["workloadsStatus"]
	workloadsStatus.Type = "array[workloadStatus]"
	status.ResourceFields["workloadsStatus"] = workloadsStatus
}

func backupVolumeSchema(backupVolume *client.Schema) {
	backupVolume.CollectionMethods = []string{"GET"}
	backupVolume.ResourceMethods = []string{"GET", "DELETE"}
	backupVolume.ResourceActions = map[string]client.Action{
		"backupList": {
			Output: "backupListOutput",
		},
		"backupGet": {
			Input:  "backupInput",
			Output: "backup",
		},
		"backupDelete": {
			Input:  "backupInput",
			Output: "backupVolume",
		},
	}
}

func settingSchema(setting *client.Schema) {
	setting.CollectionMethods = []string{"GET"}
	setting.ResourceMethods = []string{"GET", "PUT"}

	settingName := setting.ResourceFields["name"]
	settingName.Required = true
	settingName.Unique = true
	setting.ResourceFields["name"] = settingName

	settingValue := setting.ResourceFields["value"]
	settingValue.Required = true
	settingValue.Update = true
	setting.ResourceFields["value"] = settingValue

	setting.ResourceFields["definition"] = client.Field{
		Type:     "settingDefinition",
		Nullable: false,
	}
}

func volumeSchema(volume *client.Schema) {
	volume.CollectionMethods = []string{"GET", "POST"}
	volume.ResourceMethods = []string{"GET", "DELETE"}
	volume.ResourceActions = map[string]client.Action{
		"attach": {
			Input:  "attachInput",
			Output: "volume",
		},
		"detach": {
			Input:  "detachInput",
			Output: "volume",
		},
		"salvage": {
			Input:  "salvageInput",
			Output: "volume",
		},
		"activate": {
			Input:  "activateInput",
			Output: "volume",
		},
		"expand": {
			Input:  "expandInput",
			Output: "volume",
		},
		"cancelExpansion": {
			Output: "volume",
		},
		"trimFilesystem": {
			Output: "volume",
		},

		"snapshotPurge": {
			Output: "volume",
		},
		"snapshotCreate": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotGet": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotList": {
			Output: "snapshotListOutput",
		},
		"snapshotDelete": {
			Input:  "snapshotInput",
			Output: "volume",
		},
		"snapshotRevert": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotBackup": {
			Input:  "snapshotInput",
			Output: "volume",
		},

		"snapshotCRCreate": {
			Input:  "snapshotCRInput",
			Output: "snapshotCR",
		},
		"snapshotCRGet": {
			Input:  "snapshotCRInput",
			Output: "snapshotCR",
		},
		"snapshotCRList": {
			Output: "snapshotCRListOutput",
		},
		"snapshotCRDelete": {
			Input:  "snapshotCRInput",
			Output: "empty",
		},

		"recurringJobAdd": {
			Input:  "volumeRecurringJobInput",
			Output: "volumeRecurringJob",
		},

		"recurringJobList": {
			Output: "volumeRecurringJob",
		},

		"recurringJobDelete": {
			Input:  "volumeRecurringJobInput",
			Output: "volumeRecurringJob",
		},

		"updateReplicaCount": {
			Input: "UpdateReplicaCountInput",
		},

		"updateReplicaAutoBalance": {
			Input: "ReplicaAutoBalance",
		},

		"updateDataLocality": {
			Input: "UpdateDataLocalityInput",
		},

		"updateAccessMode": {
			Input:  "UpdateAccessModeInput",
			Output: "volume",
		},

		"updateSnapshotDataIntegrity": {
			Input: "UpdateSnapshotDataIntegrityInput",
		},

		"updateOfflineReplicaRebuilding": {
			Input: "UpdateOfflineReplicaRebuildingInput",
		},

		"updateBackupCompressionMethod": {
			Input: "UpdateBackupCompressionMethodInput",
		},

		"updateUnmapMarkSnapChainRemoved": {
			Input: "UpdateUnmapMarkSnapChainRemovedInput",
		},

		"updateReplicaSoftAntiAffinity": {
			Input: "UpdateReplicaSoftAntiAffinityInput",
		},

		"updateReplicaZoneSoftAntiAffinity": {
			Input: "UpdateReplicaZoneSoftAntiAffinityInput",
		},

		"updateReplicaDiskSoftAntiAffinity": {
			Input: "UpdateReplicaDiskSoftAntiAffinityInput",
		},

		"pvCreate": {
			Input:  "PVCreateInput",
			Output: "volume",
		},

		"pvcCreate": {
			Input:  "PVCCreateInput",
			Output: "volume",
		},

		"jobList": {},

		"replicaRemove": {
			Input:  "replicaRemoveInput",
			Output: "volume",
		},

		"engineUpgrade": {
			Input: "engineUpgradeInput",
		},
	}
	volume.ResourceFields["controllers"] = client.Field{
		Type:     "array[controller]",
		Nullable: true,
	}
	volumeName := volume.ResourceFields["name"]
	volumeName.Create = true
	volumeName.Required = true
	volumeName.Unique = true
	volume.ResourceFields["name"] = volumeName

	volumeSize := volume.ResourceFields["size"]
	volumeSize.Create = true
	volumeSize.Required = true
	volumeSize.Default = "100G"
	volume.ResourceFields["size"] = volumeSize

	volumeFrontend := volume.ResourceFields["frontend"]
	volumeFrontend.Create = true
	volume.ResourceFields["frontend"] = volumeFrontend

	volumeFromBackup := volume.ResourceFields["fromBackup"]
	volumeFromBackup.Create = true
	volume.ResourceFields["fromBackup"] = volumeFromBackup

	volumeDataSource := volume.ResourceFields["dataSource"]
	volumeDataSource.Create = true
	volume.ResourceFields["dataSource"] = volumeDataSource

	volumeNumberOfReplicas := volume.ResourceFields["numberOfReplicas"]
	volumeNumberOfReplicas.Create = true
	volumeNumberOfReplicas.Required = true
	volumeNumberOfReplicas.Default = 2
	volume.ResourceFields["numberOfReplicas"] = volumeNumberOfReplicas

	volumeDataLocality := volume.ResourceFields["dataLocality"]
	volumeDataLocality.Create = true
	volumeDataLocality.Default = longhorn.DataLocalityDisabled
	volume.ResourceFields["dataLocality"] = volumeDataLocality

	volumeSnapshotDataIntegrity := volume.ResourceFields["snapshotDataIntegrity"]
	volumeSnapshotDataIntegrity.Create = true
	volumeSnapshotDataIntegrity.Default = longhorn.SnapshotDataIntegrityIgnored
	volume.ResourceFields["snapshotDataIntegrity"] = volumeSnapshotDataIntegrity

	volumeOfflineReplicaRebuilding := volume.ResourceFields["offlineReplicaRebuilding"]
	volumeOfflineReplicaRebuilding.Create = true
	volumeOfflineReplicaRebuilding.Default = longhorn.OfflineReplicaRebuildingIgnored
	volume.ResourceFields["offlineReplicaRebuilding"] = volumeOfflineReplicaRebuilding

	volumeBackupCompressionMethod := volume.ResourceFields["backupCompressionMethod"]
	volumeBackupCompressionMethod.Create = true
	volumeBackupCompressionMethod.Default = longhorn.BackupCompressionMethodLz4
	volume.ResourceFields["backupCompressionMethod"] = volumeBackupCompressionMethod

	volumeAccessMode := volume.ResourceFields["accessMode"]
	volumeAccessMode.Create = true
	volumeAccessMode.Default = longhorn.AccessModeReadWriteOnce
	volume.ResourceFields["accessMode"] = volumeAccessMode

	volumeStaleReplicaTimeout := volume.ResourceFields["staleReplicaTimeout"]
	volumeStaleReplicaTimeout.Create = true
	volumeStaleReplicaTimeout.Default = 2880
	volume.ResourceFields["staleReplicaTimeout"] = volumeStaleReplicaTimeout

	volumeBackingImage := volume.ResourceFields["backingImage"]
	volumeBackingImage.Create = true
	volume.ResourceFields["backingImage"] = volumeBackingImage

	replicas := volume.ResourceFields["replicas"]
	replicas.Type = "array[replica]"
	volume.ResourceFields["replicas"] = replicas

	recurringJobSelector := volume.ResourceFields["recurringJobSelector"]
	recurringJobSelector.Create = true
	recurringJobSelector.Default = nil
	recurringJobSelector.Type = "array[volumeRecurringJob]"
	volume.ResourceFields["recurringJobSelector"] = recurringJobSelector

	standby := volume.ResourceFields["standby"]
	standby.Create = true
	standby.Default = false
	volume.ResourceFields["standby"] = standby

	revisionCounterDisabled := volume.ResourceFields["revisionCounterDisabled"]
	revisionCounterDisabled.Required = true
	revisionCounterDisabled.Create = true
	revisionCounterDisabled.Default = false
	volume.ResourceFields["revisionCounterDisabled"] = revisionCounterDisabled

	unmapMarkSnapChainRemoved := volume.ResourceFields["unmapMarkSnapChainRemoved"]
	unmapMarkSnapChainRemoved.Required = true
	unmapMarkSnapChainRemoved.Create = true
	unmapMarkSnapChainRemoved.Default = longhorn.UnmapMarkSnapChainRemovedIgnored
	volume.ResourceFields["unmapMarkSnapChainRemoved"] = unmapMarkSnapChainRemoved

	replicaSoftAntiAffinity := volume.ResourceFields["replicaSoftAntiAffinity"]
	replicaSoftAntiAffinity.Required = true
	replicaSoftAntiAffinity.Create = true
	replicaSoftAntiAffinity.Default = longhorn.ReplicaSoftAntiAffinityDefault
	volume.ResourceFields["replicaSoftAntiAffinity"] = replicaSoftAntiAffinity

	replicaZoneSoftAntiAffinity := volume.ResourceFields["replicaZoneSoftAntiAffinity"]
	replicaZoneSoftAntiAffinity.Required = true
	replicaZoneSoftAntiAffinity.Create = true
	replicaZoneSoftAntiAffinity.Default = longhorn.ReplicaZoneSoftAntiAffinityDefault
	volume.ResourceFields["replicaZoneSoftAntiAffinity"] = replicaZoneSoftAntiAffinity

	replicaDiskSoftAntiAffinity := volume.ResourceFields["replicaDiskSoftAntiAffinity"]
	replicaDiskSoftAntiAffinity.Required = true
	replicaDiskSoftAntiAffinity.Create = true
	replicaDiskSoftAntiAffinity.Default = longhorn.ReplicaDiskSoftAntiAffinityDefault
	volume.ResourceFields["replicaDiskSoftAntiAffinity"] = replicaDiskSoftAntiAffinity

	backendStoreDriver := volume.ResourceFields["backendStoreDriver"]
	backendStoreDriver.Required = true
	backendStoreDriver.Create = true
	backendStoreDriver.Default = longhorn.BackendStoreDriverTypeV1
	volume.ResourceFields["backendStoreDriver"] = backendStoreDriver

	conditions := volume.ResourceFields["conditions"]
	conditions.Type = "map[volumeCondition]"
	volume.ResourceFields["conditions"] = conditions

	diskSelector := volume.ResourceFields["diskSelector"]
	diskSelector.Create = true
	volume.ResourceFields["diskSelector"] = diskSelector

	nodeSelector := volume.ResourceFields["nodeSelector"]
	nodeSelector.Create = true
	volume.ResourceFields["nodeSelector"] = nodeSelector

	kubernetesStatus := volume.ResourceFields["kubernetesStatus"]
	kubernetesStatus.Type = "kubernetesStatus"
	volume.ResourceFields["kubernetesStatus"] = kubernetesStatus

	cloneStatus := volume.ResourceFields["cloneStatus"]
	cloneStatus.Type = "cloneStatus"
	volume.ResourceFields["cloneStatus"] = cloneStatus

	backupStatus := volume.ResourceFields["backupStatus"]
	backupStatus.Type = "array[backupStatus]"
	volume.ResourceFields["backupStatus"] = backupStatus

	restoreStatus := volume.ResourceFields["restoreStatus"]
	restoreStatus.Type = "array[restoreStatus]"
	volume.ResourceFields["restoreStatus"] = restoreStatus

	purgeStatus := volume.ResourceFields["purgeStatus"]
	purgeStatus.Type = "array[purgeStatus]"
	volume.ResourceFields["purgeStatus"] = purgeStatus

	rebuildStatus := volume.ResourceFields["rebuildStatus"]
	rebuildStatus.Type = "array[rebuildStatus]"
	volume.ResourceFields["rebuildStatus"] = rebuildStatus

	volumeAttachment := volume.ResourceFields["volumeAttachment"]
	volumeAttachment.Type = "volumeAttachment"
	volume.ResourceFields["volumeAttachment"] = volumeAttachment
}

func snapshotSchema(snapshot *client.Schema) {
	children := snapshot.ResourceFields["children"]
	children.Type = "map[bool]"
	snapshot.ResourceFields["children"] = children
}

func snapshotCRSchema(snapshotCR *client.Schema) {
	children := snapshotCR.ResourceFields["children"]
	children.Type = "map[bool]"
	snapshotCR.ResourceFields["children"] = children
}

func backupListOutputSchema(backupList *client.Schema) {
	data := backupList.ResourceFields["data"]
	data.Type = "array[backup]"
	backupList.ResourceFields["data"] = data
}

func snapshotListOutputSchema(snapshotList *client.Schema) {
	data := snapshotList.ResourceFields["data"]
	data.Type = "array[snapshot]"
	snapshotList.ResourceFields["data"] = data
}

func systemBackupSchema(systemBackup *client.Schema) {
	systemBackup.CollectionMethods = []string{"GET", "POST"}
	systemBackup.ResourceMethods = []string{"GET", "DELETE"}

	name := systemBackup.ResourceFields["name"]
	name.Required = true
	name.Unique = true
	name.Create = true
	systemBackup.ResourceFields["name"] = name
}

func systemRestoreSchema(systemRestore *client.Schema) {
	systemRestore.CollectionMethods = []string{"GET", "POST"}
	systemRestore.ResourceMethods = []string{"GET", "DELETE"}

	name := systemRestore.ResourceFields["name"]
	name.Required = true
	name.Unique = true
	name.Create = true
	systemRestore.ResourceFields["name"] = name

	systemBackup := systemRestore.ResourceFields["systemBackup"]
	systemBackup.Required = true
	systemBackup.Unique = true
	systemRestore.ResourceFields["systemBackup"] = systemBackup
}

func snapshotCRListOutputSchema(snapshotList *client.Schema) {
	data := snapshotList.ResourceFields["data"]
	data.Type = "array[snapshotCR]"
	snapshotList.ResourceFields["data"] = data
}

func attachmentSchema(attachment *client.Schema) {
	conditions := attachment.ResourceFields["conditions"]
	conditions.Type = "array[longhornCondition]"
	attachment.ResourceFields["conditions"] = conditions
}

func volumeAttachmentSchema(volumeAttachment *client.Schema) {
	attachments := volumeAttachment.ResourceFields["attachments"]
	attachments.Type = "map[attachment]"
	volumeAttachment.ResourceFields["attachments"] = attachments
}

func toEmptyResource() *Empty {
	return &Empty{
		Resource: client.Resource{
			Type: "empty",
		},
	}
}

func toSettingResource(setting *longhorn.Setting) *Setting {
	definition, _ := types.GetSettingDefinition(types.SettingName(setting.Name))

	return &Setting{
		Resource: client.Resource{
			Id:    setting.Name,
			Type:  "setting",
			Links: map[string]string{},
		},
		Name:  setting.Name,
		Value: setting.Value,

		Definition: definition,
	}
}

func toSettingCollection(settings []*longhorn.Setting) *client.GenericCollection {
	data := []interface{}{}
	for _, setting := range settings {
		data = append(data, toSettingResource(setting))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "setting"}}
}

func toVolumeResource(v *longhorn.Volume, ves []*longhorn.Engine, vrs []*longhorn.Replica, backups []*longhorn.Backup, lhVolumeAttachment *longhorn.VolumeAttachment, apiContext *api.ApiContext) *Volume {
	var ve *longhorn.Engine
	controllers := []Controller{}
	backupStatus := []BackupStatus{}
	restoreStatus := []RestoreStatus{}
	var purgeStatuses []PurgeStatus
	rebuildStatuses := []RebuildStatus{}
	volumeAttachment := VolumeAttachment{
		Attachments: make(map[string]Attachment),
		Volume:      v.Name,
	}
	for _, e := range ves {
		actualSize := int64(0)
		snapshots := e.Status.Snapshots
		for _, snapshot := range snapshots {
			snapshotSize, err := util.ConvertSize(snapshot.Size)
			if err != nil {
				logrus.WithError(err).Warnf("api: failed to convert snapshot size %v for volume %v", snapshot.Size, v.Name)
				continue
			}
			actualSize += snapshotSize
		}
		controllers = append(controllers, Controller{
			Instance: Instance{
				Name:                e.Name,
				Running:             e.Status.CurrentState == longhorn.InstanceStateRunning,
				NodeID:              e.Spec.NodeID,
				Address:             e.Status.IP,
				Image:               e.Spec.Image,
				CurrentImage:        e.Status.CurrentImage,
				InstanceManagerName: e.Status.InstanceManagerName,
			},
			Size:                             strconv.FormatInt(e.Status.CurrentSize, 10),
			ActualSize:                       strconv.FormatInt(actualSize, 10),
			Endpoint:                         e.Status.Endpoint,
			LastRestoredBackup:               e.Status.LastRestoredBackup,
			RequestedBackupRestore:           e.Spec.RequestedBackupRestore,
			IsExpanding:                      e.Status.IsExpanding,
			LastExpansionError:               e.Status.LastExpansionError,
			LastExpansionFailedAt:            e.Status.LastExpansionFailedAt,
			UnmapMarkSnapChainRemovedEnabled: e.Status.UnmapMarkSnapChainRemovedEnabled,
		})
		if e.Spec.NodeID == v.Status.CurrentNodeID {
			ve = e
		}
		rs := e.Status.RestoreStatus
		if rs != nil {
			replicas := util.GetSortedKeysFromMap(rs)
			for _, replica := range replicas {
				restoreStatus = append(restoreStatus, RestoreStatus{
					Resource:     client.Resource{},
					Replica:      datastore.ReplicaAddressToReplicaName(replica, vrs),
					IsRestoring:  rs[replica].IsRestoring,
					LastRestored: rs[replica].LastRestored,
					Progress:     rs[replica].Progress,
					Error:        rs[replica].Error,
					Filename:     rs[replica].Filename,
					State:        rs[replica].State,
					BackupURL:    rs[replica].BackupURL,
				})
			}
		}
		purgeStatus := e.Status.PurgeStatus
		if purgeStatus != nil {
			replicas := util.GetSortedKeysFromMap(purgeStatus)
			for _, replica := range replicas {
				purgeStatuses = append(purgeStatuses, PurgeStatus{
					Resource:  client.Resource{},
					Replica:   datastore.ReplicaAddressToReplicaName(replica, vrs),
					Error:     purgeStatus[replica].Error,
					IsPurging: purgeStatus[replica].IsPurging,
					Progress:  purgeStatus[replica].Progress,
					State:     purgeStatus[replica].State,
				})
			}
		}
		rebuildStatus := e.Status.RebuildStatus
		if rebuildStatus != nil {
			replicas := util.GetSortedKeysFromMap(rebuildStatus)
			for _, replica := range replicas {
				rebuildStatuses = append(rebuildStatuses, RebuildStatus{
					Resource:     client.Resource{},
					Replica:      datastore.ReplicaAddressToReplicaName(replica, vrs),
					Error:        rebuildStatus[replica].Error,
					IsRebuilding: rebuildStatus[replica].IsRebuilding,
					Progress:     rebuildStatus[replica].Progress,
					State:        rebuildStatus[replica].State,
					FromReplica:  datastore.ReplicaAddressToReplicaName(rebuildStatus[replica].FromReplicaAddress, vrs),
				})
			}
		}
	}

	replicas := []Replica{}
	for _, r := range vrs {
		mode := ""
		if ve != nil && ve.Status.ReplicaModeMap != nil {
			mode = string(ve.Status.ReplicaModeMap[r.Name])
		}
		replicas = append(replicas, Replica{
			Instance: Instance{
				Name:                r.Name,
				Running:             r.Status.CurrentState == longhorn.InstanceStateRunning,
				Address:             r.Status.IP,
				NodeID:              r.Spec.NodeID,
				Image:               r.Spec.Image,
				CurrentImage:        r.Status.CurrentImage,
				InstanceManagerName: r.Status.InstanceManagerName,
			},
			DiskID:             r.Spec.DiskID,
			DiskPath:           r.Spec.DiskPath,
			DataPath:           types.GetReplicaDataPath(r.Spec.DiskPath, r.Spec.DataDirectoryName),
			Mode:               mode,
			FailedAt:           r.Spec.FailedAt,
			BackendStoreDriver: string(r.Spec.BackendStoreDriver),
		})
	}

	for _, b := range backups {
		backupStatus = append(backupStatus, BackupStatus{
			Resource:  client.Resource{},
			Name:      b.Name,
			Snapshot:  b.Status.SnapshotName,
			Progress:  b.Status.Progress,
			BackupURL: b.Status.URL,
			Error:     b.Status.Error,
			State:     string(b.Status.State),
			Replica:   datastore.ReplicaAddressToReplicaName(b.Status.ReplicaAddress, vrs),
			Size:      b.Status.Size,
		})
	}

	if lhVolumeAttachment != nil {
		for k, v := range lhVolumeAttachment.Spec.AttachmentTickets {
			if v != nil {
				volumeAttachment.Attachments[k] = Attachment{
					AttachmentID:   v.ID,
					AttachmentType: string(v.Type),
					NodeID:         v.NodeID,
					Parameters:     v.Parameters,
				}
			}
		}
		for k, v := range lhVolumeAttachment.Status.AttachmentTicketStatuses {
			if v == nil {
				continue
			}
			attachment, ok := volumeAttachment.Attachments[k]
			if !ok {
				continue
			}
			attachment.Satisfied = longhorn.IsAttachmentTicketSatisfied(attachment.AttachmentID, lhVolumeAttachment)
			attachment.Conditions = v.Conditions
			volumeAttachment.Attachments[k] = attachment
		}
	}

	// The volume is not ready for workloads if:
	//   1. It's auto attached.
	//   2. It fails to schedule replicas during the volume creation,
	//      in which case scheduling failure will happen when the volume is detached.
	//      In other cases, scheduling failure only happens when the volume is attached.
	//   3. It's faulted.
	//   4. It's restore pending.
	//   5. It's failed to clone
	ready := true
	scheduledCondition := types.GetCondition(v.Status.Conditions, longhorn.VolumeConditionTypeScheduled)
	if (v.Spec.NodeID == "" && v.Status.State != longhorn.VolumeStateDetached) ||
		(v.Status.State == longhorn.VolumeStateDetached && scheduledCondition.Status != longhorn.ConditionStatusTrue) ||
		v.Status.Robustness == longhorn.VolumeRobustnessFaulted ||
		v.Status.RestoreRequired ||
		v.Status.CloneStatus.State == longhorn.VolumeCloneStateFailed {
		ready = false
	}

	r := &Volume{
		Resource: client.Resource{
			Id:      v.Name,
			Type:    "volume",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:                      v.Name,
		Size:                      strconv.FormatInt(v.Spec.Size, 10),
		Frontend:                  v.Spec.Frontend,
		DisableFrontend:           v.Spec.DisableFrontend,
		LastAttachedBy:            v.Spec.LastAttachedBy,
		FromBackup:                v.Spec.FromBackup,
		DataSource:                v.Spec.DataSource,
		NumberOfReplicas:          v.Spec.NumberOfReplicas,
		ReplicaAutoBalance:        v.Spec.ReplicaAutoBalance,
		DataLocality:              v.Spec.DataLocality,
		SnapshotDataIntegrity:     v.Spec.SnapshotDataIntegrity,
		BackupCompressionMethod:   v.Spec.BackupCompressionMethod,
		StaleReplicaTimeout:       v.Spec.StaleReplicaTimeout,
		Created:                   v.CreationTimestamp.String(),
		Image:                     v.Spec.Image,
		BackingImage:              v.Spec.BackingImage,
		Standby:                   v.Spec.Standby,
		DiskSelector:              v.Spec.DiskSelector,
		NodeSelector:              v.Spec.NodeSelector,
		RestoreVolumeRecurringJob: v.Spec.RestoreVolumeRecurringJob,

		State:                            v.Status.State,
		Robustness:                       v.Status.Robustness,
		CurrentImage:                     v.Status.CurrentImage,
		LastBackup:                       v.Status.LastBackup,
		LastBackupAt:                     v.Status.LastBackupAt,
		RestoreRequired:                  v.Status.RestoreRequired,
		RestoreInitiated:                 v.Status.RestoreInitiated,
		RevisionCounterDisabled:          v.Spec.RevisionCounterDisabled,
		UnmapMarkSnapChainRemoved:        v.Spec.UnmapMarkSnapChainRemoved,
		ReplicaSoftAntiAffinity:          v.Spec.ReplicaSoftAntiAffinity,
		ReplicaZoneSoftAntiAffinity:      v.Spec.ReplicaZoneSoftAntiAffinity,
		ReplicaDiskSoftAntiAffinity:      v.Spec.ReplicaDiskSoftAntiAffinity,
		BackendStoreDriver:               v.Spec.BackendStoreDriver,
		OfflineReplicaRebuilding:         v.Spec.OfflineReplicaRebuilding,
		OfflineReplicaRebuildingRequired: v.Status.OfflineReplicaRebuildingRequired,
		Ready:                            ready,

		AccessMode:    v.Spec.AccessMode,
		ShareEndpoint: v.Status.ShareEndpoint,
		ShareState:    v.Status.ShareState,

		Migratable: v.Spec.Migratable,

		Encrypted: v.Spec.Encrypted,

		Conditions:       sliceToMap(v.Status.Conditions),
		KubernetesStatus: v.Status.KubernetesStatus,
		CloneStatus:      v.Status.CloneStatus,

		Controllers:      controllers,
		Replicas:         replicas,
		BackupStatus:     backupStatus,
		RestoreStatus:    restoreStatus,
		PurgeStatus:      purgeStatuses,
		RebuildStatus:    rebuildStatuses,
		VolumeAttachment: volumeAttachment,
	}

	// api attach & detach calls are always allowed
	// the volume manager is responsible for handling them appropriately
	actions := map[string]struct{}{
		"attach": {},
		"detach": {},
	}

	if v.Status.Robustness == longhorn.VolumeRobustnessFaulted {
		actions["salvage"] = struct{}{}
	} else {

		actions["snapshotCRCreate"] = struct{}{}
		actions["snapshotCRGet"] = struct{}{}
		actions["snapshotCRList"] = struct{}{}
		actions["snapshotCRDelete"] = struct{}{}
		actions["snapshotBackup"] = struct{}{}

		switch v.Status.State {
		case longhorn.VolumeStateDetached:
			actions["activate"] = struct{}{}
			actions["expand"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
			actions["updateAccessMode"] = struct{}{}
			actions["updateReplicaAutoBalance"] = struct{}{}
			actions["updateUnmapMarkSnapChainRemoved"] = struct{}{}
			actions["updateSnapshotDataIntegrity"] = struct{}{}
			actions["updateOfflineReplicaRebuilding"] = struct{}{}
			actions["updateBackupCompressionMethod"] = struct{}{}
			actions["updateReplicaSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaZoneSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaDiskSoftAntiAffinity"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		case longhorn.VolumeStateAttaching:
			actions["cancelExpansion"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		case longhorn.VolumeStateAttached:
			actions["activate"] = struct{}{}
			actions["expand"] = struct{}{}
			actions["snapshotPurge"] = struct{}{}
			actions["snapshotCreate"] = struct{}{}
			actions["snapshotList"] = struct{}{}
			actions["snapshotGet"] = struct{}{}
			actions["snapshotDelete"] = struct{}{}
			actions["snapshotRevert"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["updateReplicaCount"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
			actions["updateReplicaAutoBalance"] = struct{}{}
			actions["updateUnmapMarkSnapChainRemoved"] = struct{}{}
			actions["updateSnapshotDataIntegrity"] = struct{}{}
			actions["updateOfflineReplicaRebuilding"] = struct{}{}
			actions["updateBackupCompressionMethod"] = struct{}{}
			actions["updateReplicaSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaZoneSoftAntiAffinity"] = struct{}{}
			actions["updateReplicaDiskSoftAntiAffinity"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
			actions["trimFilesystem"] = struct{}{}
			actions["recurringJobAdd"] = struct{}{}
			actions["recurringJobDelete"] = struct{}{}
			actions["recurringJobList"] = struct{}{}
		}
	}

	for action := range actions {
		r.Actions[action] = apiContext.UrlBuilder.ActionLink(r.Resource, action)
	}

	return r
}

func toSnapshotCRResource(s *longhorn.Snapshot) *SnapshotCR {
	if s == nil {
		return nil
	}

	getLabels := func() map[string]string {
		if len(s.Spec.Labels) > 0 {
			return s.Spec.Labels
		}
		return s.Status.Labels
	}

	return &SnapshotCR{
		Resource: client.Resource{
			Id:   s.Name,
			Type: "snapshotCR",
		},
		Name:           s.Name,
		CRCreationTime: s.CreationTimestamp.Format(time.RFC3339),
		Volume:         s.Spec.Volume,
		CreateSnapshot: s.Spec.CreateSnapshot,
		Parent:         s.Status.Parent,
		Children:       s.Status.Children,
		MarkRemoved:    s.Status.MarkRemoved,
		UserCreated:    s.Status.UserCreated,
		CreationTime:   s.Status.CreationTime,
		Size:           s.Status.Size,
		Labels:         getLabels(),
		OwnerID:        s.Status.OwnerID,
		Error:          s.Status.Error,
		RestoreSize:    s.Status.RestoreSize,
		ReadyToUse:     s.Status.ReadyToUse,
		Checksum:       s.Status.Checksum,
	}
}

func toSnapshotCRCollection(snapCRs map[string]*longhorn.Snapshot) *client.GenericCollection {
	data := []interface{}{}

	for _, v := range snapCRs {
		data = append(data, toSnapshotCRResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "snapshotCR"}}
}

func toSnapshotResource(s *longhorn.SnapshotInfo, checksum string) *Snapshot {
	if s == nil {
		return nil
	}
	return &Snapshot{
		Resource: client.Resource{
			Id:   s.Name,
			Type: "snapshot",
		},
		SnapshotInfo: *s,
		Checksum:     checksum,
	}
}

func toSnapshotCollection(ssList map[string]*longhorn.SnapshotInfo, ssListRO map[string]*longhorn.Snapshot) *client.GenericCollection {
	data := []interface{}{}

	for name, v := range ssList {
		checksum := ""
		if ssListRO != nil {
			if ssRO, ok := ssListRO[name]; ok {
				checksum = ssRO.Status.Checksum
			}
		}
		data = append(data, toSnapshotResource(v, checksum))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "snapshot"}}
}

func toVolumeRecurringJobResource(obj *longhorn.VolumeRecurringJob) *VolumeRecurringJob {
	if obj == nil {
		return nil
	}
	return &VolumeRecurringJob{
		Resource: client.Resource{
			Id:   obj.Name,
			Type: "volumeRecurringJob",
		},
		VolumeRecurringJob: *obj,
	}
}

func toVolumeRecurringJobCollection(recurringJobs map[string]*longhorn.VolumeRecurringJob) *client.GenericCollection {
	data := []interface{}{}
	for _, recurringJob := range recurringJobs {
		data = append(data, toVolumeRecurringJobResource(recurringJob))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "volumeRecurringJob"}}
}

func toBackupTargetResource(bt *longhorn.BackupTarget) *BackupTarget {
	if bt == nil {
		return nil
	}

	res := &BackupTarget{
		Resource: client.Resource{
			Id:    bt.Name,
			Type:  "backupTarget",
			Links: map[string]string{},
		},
		BackupTarget: engineapi.BackupTarget{
			BackupTargetURL:  bt.Spec.BackupTargetURL,
			CredentialSecret: bt.Spec.CredentialSecret,
			PollInterval:     bt.Spec.PollInterval.Duration.String(),
			Available:        bt.Status.Available,
			Message:          types.GetCondition(bt.Status.Conditions, longhorn.BackupTargetConditionTypeUnavailable).Message,
		},
	}
	return res
}

func toBackupVolumeResource(bv *longhorn.BackupVolume, apiContext *api.ApiContext) *BackupVolume {
	if bv == nil {
		return nil
	}
	b := &BackupVolume{
		Resource: client.Resource{
			Id:    bv.Name,
			Type:  "backupVolume",
			Links: map[string]string{},
		},
		Name:                 bv.Name,
		Size:                 bv.Status.Size,
		Labels:               bv.Status.Labels,
		Created:              bv.Status.CreatedAt,
		LastBackupName:       bv.Status.LastBackupName,
		LastBackupAt:         bv.Status.LastBackupAt,
		DataStored:           bv.Status.DataStored,
		Messages:             bv.Status.Messages,
		BackingImageName:     bv.Status.BackingImageName,
		BackingImageChecksum: bv.Status.BackingImageChecksum,
		StorageClassName:     bv.Status.StorageClassName,
	}
	b.Actions = map[string]string{
		"backupList":   apiContext.UrlBuilder.ActionLink(b.Resource, "backupList"),
		"backupGet":    apiContext.UrlBuilder.ActionLink(b.Resource, "backupGet"),
		"backupDelete": apiContext.UrlBuilder.ActionLink(b.Resource, "backupDelete"),
	}
	return b
}

func toBackupTargetCollection(bts []*longhorn.BackupTarget) *client.GenericCollection {
	data := []interface{}{}
	for _, bt := range bts {
		data = append(data, toBackupTargetResource(bt))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backupTarget"}}
}

func toBackupVolumeCollection(bvs []*longhorn.BackupVolume, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, bv := range bvs {
		data = append(data, toBackupVolumeResource(bv, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backupVolume"}}
}

func toBackupResource(b *longhorn.Backup) *Backup {
	if b == nil {
		return nil
	}

	getSnapshotNameFromBackup := func(b *longhorn.Backup) string {
		if b.Spec.SnapshotName != "" {
			return b.Spec.SnapshotName
		}
		return b.Status.SnapshotName
	}

	ret := &Backup{
		Resource: client.Resource{
			Id:    b.Name,
			Type:  "backup",
			Links: map[string]string{},
		},
		Name:                   b.Name,
		State:                  b.Status.State,
		Progress:               b.Status.Progress,
		Error:                  b.Status.Error,
		URL:                    b.Status.URL,
		SnapshotName:           getSnapshotNameFromBackup(b),
		SnapshotCreated:        b.Status.SnapshotCreatedAt,
		Created:                b.Status.BackupCreatedAt,
		Size:                   b.Status.Size,
		Labels:                 b.Status.Labels,
		Messages:               b.Status.Messages,
		VolumeName:             b.Status.VolumeName,
		VolumeSize:             b.Status.VolumeSize,
		VolumeCreated:          b.Status.VolumeCreated,
		VolumeBackingImageName: b.Status.VolumeBackingImageName,
		CompressionMethod:      string(b.Status.CompressionMethod),
	}
	// Set the volume name from backup CR's label if it's empty.
	// This field is empty probably because the backup state is not Ready
	// or the content of the backup config is empty.
	if ret.VolumeName == "" {
		backupVolumeName, ok := b.Labels[types.LonghornLabelBackupVolume]
		if ok {
			ret.VolumeName = backupVolumeName
		}
	}
	return ret
}

func toBackupCollection(bs []*longhorn.Backup) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range bs {
		data = append(data, toBackupResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backup"}}
}

func toEngineImageResource(ei *longhorn.EngineImage, isDefault bool) *EngineImage {
	return &EngineImage{
		Resource: client.Resource{
			Id:    ei.Name,
			Type:  "engineImage",
			Links: map[string]string{},
		},
		Name:              ei.Name,
		Image:             ei.Spec.Image,
		Default:           isDefault,
		EngineImageStatus: ei.Status,
	}
}

func toEngineImageCollection(eis []*longhorn.EngineImage, defaultImage string) *client.GenericCollection {
	data := []interface{}{}
	for _, ei := range eis {
		isDefault := (ei.Spec.Image == defaultImage)
		data = append(data, toEngineImageResource(ei, isDefault))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "engineImage"}}
}

func toBackingImageResource(bi *longhorn.BackingImage, apiContext *api.ApiContext) *BackingImage {
	deletionTimestamp := ""
	if bi.DeletionTimestamp != nil {
		deletionTimestamp = bi.DeletionTimestamp.String()
	}
	diskFileStatusMap := make(map[string]longhorn.BackingImageDiskFileStatus)
	if bi.Status.DiskFileStatusMap != nil {
		for diskUUID, diskStatus := range bi.Status.DiskFileStatusMap {
			diskFileStatusMap[diskUUID] = *diskStatus
		}
	}
	if bi.Spec.Disks != nil {
		for diskUUID := range bi.Spec.Disks {
			if _, exists := bi.Status.DiskFileStatusMap[diskUUID]; !exists {
				diskFileStatusMap[diskUUID] = longhorn.BackingImageDiskFileStatus{
					Message: "File processing is not started",
				}
			}
		}
	}
	res := &BackingImage{
		Resource: client.Resource{
			Id:    bi.Name,
			Type:  "backingImage",
			Links: map[string]string{},
		},

		Name:             bi.Name,
		UUID:             bi.Status.UUID,
		ExpectedChecksum: bi.Spec.Checksum,
		SourceType:       string(bi.Spec.SourceType),
		Parameters:       bi.Spec.SourceParameters,

		DiskFileStatusMap: diskFileStatusMap,
		Size:              bi.Status.Size,
		CurrentChecksum:   bi.Status.Checksum,

		DeletionTimestamp: deletionTimestamp,
	}
	res.Actions = map[string]string{
		"backingImageCleanup": apiContext.UrlBuilder.ActionLink(res.Resource, "backingImageCleanup"),
		BackingImageUpload:    apiContext.UrlBuilder.ActionLink(res.Resource, BackingImageUpload),
	}
	return res
}

func toBackingImageCollection(bis []*longhorn.BackingImage, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, bi := range bis {
		data = append(data, toBackingImageResource(bi, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backingImage"}}
}

type Server struct {
	m   *manager.VolumeManager
	wsc *controller.WebsocketController
	fwd *Fwd
}

func NewServer(m *manager.VolumeManager, wsc *controller.WebsocketController) *Server {
	s := &Server{
		m:   m,
		wsc: wsc,
		fwd: NewFwd(m),
	}
	return s
}

func toNodeResource(node *longhorn.Node, address string, apiContext *api.ApiContext) *Node {
	n := &Node{
		Resource: client.Resource{
			Id:      node.Name,
			Type:    "node",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:                      node.Name,
		Address:                   address,
		AllowScheduling:           node.Spec.AllowScheduling,
		EvictionRequested:         node.Spec.EvictionRequested,
		Conditions:                sliceToMap(node.Status.Conditions),
		Tags:                      node.Spec.Tags,
		Region:                    node.Status.Region,
		Zone:                      node.Status.Zone,
		InstanceManagerCPURequest: node.Spec.InstanceManagerCPURequest,
		AutoEvicting:              node.Status.AutoEvicting,
	}

	disks := map[string]DiskInfo{}
	for name, disk := range node.Spec.Disks {
		di := DiskInfo{
			DiskSpec: disk,
		}
		if node.Status.DiskStatus != nil && node.Status.DiskStatus[name] != nil {
			di.DiskStatus = DiskStatus{
				Conditions:       sliceToMap(node.Status.DiskStatus[name].Conditions),
				StorageAvailable: node.Status.DiskStatus[name].StorageAvailable,
				StorageScheduled: node.Status.DiskStatus[name].StorageScheduled,
				StorageMaximum:   node.Status.DiskStatus[name].StorageMaximum,
				ScheduledReplica: node.Status.DiskStatus[name].ScheduledReplica,
				DiskUUID:         node.Status.DiskStatus[name].DiskUUID,
			}
		}
		disks[name] = di
	}
	n.Disks = disks

	n.Actions = map[string]string{
		"diskUpdate": apiContext.UrlBuilder.ActionLink(n.Resource, "diskUpdate"),
	}

	return n
}

func toNodeCollection(nodeList []*longhorn.Node, nodeIPMap map[string]string, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, node := range nodeList {
		data = append(data, toNodeResource(node, nodeIPMap[node.Name], apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "node"}}
}

func toEventResource(event corev1.Event) *Event {
	e := &Event{
		Resource: client.Resource{
			Id:    event.Name,
			Type:  "event",
			Links: map[string]string{},
		},
		Event:     event,
		EventType: event.Type,
	}
	return e
}

func toEventCollection(eventList *corev1.EventList) *client.GenericCollection {
	data := []interface{}{}
	for _, event := range eventList.Items {
		data = append(data, toEventResource(event))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "event"}}
}

func toSupportBundleCollection(supportBundles []*longhorn.SupportBundle, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, supportBundle := range supportBundles {
		supportBundleError := types.GetCondition(supportBundle.Status.Conditions, longhorn.SupportBundleConditionTypeError)
		data = append(data, toSupportBundleResource(supportBundle.Status.OwnerID, &manager.SupportBundle{
			Name:               supportBundle.Name,
			State:              supportBundle.Status.State,
			Filename:           supportBundle.Status.Filename,
			Size:               supportBundle.Status.Filesize,
			ProgressPercentage: supportBundle.Status.Progress,
			Error:              fmt.Sprintf("%v: %v", supportBundleError.Reason, supportBundleError.Message),
		}))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "supportBundle"}}
}

func toSupportBundleResource(nodeID string, supportBundle *manager.SupportBundle) *SupportBundle {
	return &SupportBundle{
		Resource: client.Resource{
			Id:   nodeID,
			Type: "supportBundle",
		},
		NodeID:             nodeID,
		State:              supportBundle.State,
		Name:               supportBundle.Name,
		ErrorMessage:       supportBundle.Error,
		ProgressPercentage: supportBundle.ProgressPercentage,
	}
}

func toSystemBackupCollection(systemBackups []*longhorn.SystemBackup) *client.GenericCollection {
	data := []interface{}{}
	for _, systemBackup := range systemBackups {
		data = append(data, toSystemBackupResource(systemBackup))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "systemBackup"}}
}

func toSystemBackupResource(systemBackup *longhorn.SystemBackup) *SystemBackup {
	err := ""
	if systemBackup.Status.State == longhorn.SystemBackupStateError {
		errCondition := types.GetCondition(systemBackup.Status.Conditions, longhorn.SystemBackupConditionTypeError)
		if errCondition.Status == longhorn.ConditionStatusTrue {
			err = fmt.Sprintf("%v: %v", errCondition.Reason, errCondition.Message)
		}
	}
	return &SystemBackup{
		Resource: client.Resource{
			Id:   systemBackup.Name,
			Type: "systemBackup",
		},
		Name:               systemBackup.Name,
		VolumeBackupPolicy: systemBackup.Spec.VolumeBackupPolicy,

		Version:      systemBackup.Status.Version,
		ManagerImage: systemBackup.Status.ManagerImage,
		State:        systemBackup.Status.State,
		CreatedAt:    systemBackup.Status.CreatedAt.String(),
		Error:        err,
	}
}

func toSystemRestoreCollection(systemRestores []*longhorn.SystemRestore) *client.GenericCollection {
	data := []interface{}{}
	for _, systemRestore := range systemRestores {
		data = append(data, toSystemRestoreResource(systemRestore))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "systemRestore"}}
}

func toSystemRestoreResource(systemRestore *longhorn.SystemRestore) *SystemRestore {
	err := ""
	if systemRestore.Status.State == longhorn.SystemRestoreStateError {
		errCondition := types.GetCondition(systemRestore.Status.Conditions, longhorn.SystemRestoreConditionTypeError)
		if errCondition.Status == longhorn.ConditionStatusTrue {
			err = fmt.Sprintf("%v: %v", errCondition.Reason, errCondition.Message)
		}
	}
	return &SystemRestore{
		Resource: client.Resource{
			Id:   systemRestore.Name,
			Type: "systemRestore",
		},
		Name:         systemRestore.Name,
		SystemBackup: systemRestore.Spec.SystemBackup,
		State:        systemRestore.Status.State,
		CreatedAt:    systemRestore.CreationTimestamp.String(),
		Error:        err,
	}
}

func toTagResource(tag string, tagType string, apiContext *api.ApiContext) *Tag {
	t := &Tag{
		Resource: client.Resource{
			Id:    tag,
			Links: map[string]string{},
			Type:  "tag",
		},
		Name:    tag,
		TagType: tagType,
	}
	t.Links["self"] = apiContext.UrlBuilder.Current()
	return t
}

func toTagCollection(tags []string, tagType string, apiContext *api.ApiContext) *client.GenericCollection {
	var data []interface{}
	for _, tag := range tags {
		data = append(data, toTagResource(tag, tagType, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "tag"}}
}

func toInstanceManagerResource(im *longhorn.InstanceManager) *InstanceManager {
	return &InstanceManager{
		Resource: client.Resource{
			Id:   im.Name,
			Type: "instanceManager",
		},
		CurrentState:       im.Status.CurrentState,
		Image:              im.Spec.Image,
		Name:               im.Name,
		NodeID:             im.Spec.NodeID,
		ManagerType:        string(im.Spec.Type),
		BackendStoreDriver: string(im.Spec.BackendStoreDriver),
		InstanceEngines:    im.Status.InstanceEngines,
		InstanceReplicas:   im.Status.InstanceReplicas,
		Instances:          im.Status.Instances,
	}
}

func toInstanceManagerCollection(instanceManagers map[string]*longhorn.InstanceManager) *client.GenericCollection {
	var data []interface{}
	for _, im := range instanceManagers {
		data = append(data, toInstanceManagerResource(im))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "instanceManager"}}
}

func toRecurringJobResource(recurringJob *longhorn.RecurringJob, apiContext *api.ApiContext) *RecurringJob {
	return &RecurringJob{
		Resource: client.Resource{
			Id:   recurringJob.Name,
			Type: "recurringJob",
		},
		RecurringJobSpec: longhorn.RecurringJobSpec{
			Name:        recurringJob.Name,
			Groups:      recurringJob.Spec.Groups,
			Task:        recurringJob.Spec.Task,
			Cron:        recurringJob.Spec.Cron,
			Retain:      recurringJob.Spec.Retain,
			Concurrency: recurringJob.Spec.Concurrency,
			Labels:      recurringJob.Spec.Labels,
		},
	}
}

func toRecurringJobCollection(jobs []*longhorn.RecurringJob, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, job := range jobs {
		data = append(data, toRecurringJobResource(job, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "recurringJob"}}
}

func toOrphanResource(orphan *longhorn.Orphan) *Orphan {
	return &Orphan{
		Resource: client.Resource{
			Id:   orphan.Name,
			Type: "orphan",
		},
		Name: orphan.Name,
		OrphanSpec: longhorn.OrphanSpec{
			NodeID:     orphan.Spec.NodeID,
			Type:       orphan.Spec.Type,
			Parameters: orphan.Spec.Parameters,
		},
	}
}

func toOrphanCollection(orphans map[string]*longhorn.Orphan) *client.GenericCollection {
	var data []interface{}
	for _, orphan := range orphans {
		data = append(data, toOrphanResource(orphan))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "orphan"}}
}

func sliceToMap(conditions []longhorn.Condition) map[string]longhorn.Condition {
	converted := map[string]longhorn.Condition{}
	for _, c := range conditions {
		converted[c.Type] = c
	}
	return converted
}
