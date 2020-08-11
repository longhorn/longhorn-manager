package api

import (
	"strconv"
	"time"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

type Volume struct {
	client.Resource

	Name                string                 `json:"name"`
	Size                string                 `json:"size"`
	Frontend            types.VolumeFrontend   `json:"frontend"`
	DisableFrontend     bool                   `json:"disableFrontend"`
	FromBackup          string                 `json:"fromBackup"`
	NumberOfReplicas    int                    `json:"numberOfReplicas"`
	DataLocality        types.DataLocality     `json:"dataLocality"`
	StaleReplicaTimeout int                    `json:"staleReplicaTimeout"`
	State               types.VolumeState      `json:"state"`
	Robustness          types.VolumeRobustness `json:"robustness"`
	EngineImage         string                 `json:"engineImage"`
	CurrentImage        string                 `json:"currentImage"`
	BaseImage           string                 `json:"baseImage"`
	Created             string                 `json:"created"`
	LastBackup          string                 `json:"lastBackup"`
	LastBackupAt        string                 `json:"lastBackupAt"`
	Standby             bool                   `json:"standby"`
	RestoreRequired     bool                   `json:"restoreRequired"`
	DiskSelector        []string               `json:"diskSelector"`
	NodeSelector        []string               `json:"nodeSelector"`

	RecurringJobs    []types.RecurringJob       `json:"recurringJobs"`
	Conditions       map[string]types.Condition `json:"conditions"`
	KubernetesStatus types.KubernetesStatus     `json:"kubernetesStatus"`
	Ready            bool                       `json:"ready"`

	Replicas      []Replica       `json:"replicas"`
	Controllers   []Controller    `json:"controllers"`
	BackupStatus  []BackupStatus  `json:"backupStatus"`
	RestoreStatus []RestoreStatus `json:"restoreStatus"`
	PurgeStatus   []PurgeStatus   `json:"purgeStatus"`
	RebuildStatus []RebuildStatus `json:"rebuildStatus"`

	Timestamp string `json:"timestamp"`
}

type Snapshot struct {
	client.Resource
	types.Snapshot
}

type BackupVolume struct {
	client.Resource
	engineapi.BackupVolume
}

type Backup struct {
	client.Resource
	engineapi.Backup
}

type Setting struct {
	client.Resource
	Name       string                  `json:"name"`
	Value      string                  `json:"value"`
	Definition types.SettingDefinition `json:"definition"`

	Timestamp string `json:"timestamp"`
}

type Instance struct {
	Name                string `json:"name"`
	NodeID              string `json:"hostId"`
	Address             string `json:"address"`
	Running             bool   `json:"running"`
	EngineImage         string `json:"engineImage"`
	CurrentImage        string `json:"currentImage"`
	InstanceManagerName string `json:"instanceManagerName"`
}

type Controller struct {
	Instance
	Size                   string `json:"size"`
	Endpoint               string `json:"endpoint"`
	LastRestoredBackup     string `json:"lastRestoredBackup"`
	RequestedBackupRestore string `json:"requestedBackupRestore"`
	IsExpanding            bool   `json:"isExpanding"`
	LastExpansionError     string `json:"lastExpansionError"`
	LastExpansionFailedAt  string `json:"lastExpansionFailedAt"`
}

type Replica struct {
	Instance

	DiskID   string `json:"diskID"`
	DataPath string `json:"dataPath"`
	Mode     string `json:"mode"`
	FailedAt string `json:"failedAt"`
}

type EngineImage struct {
	client.Resource

	Name    string `json:"name"`
	Image   string `json:"image"`
	Default bool   `json:"default"`
	types.EngineImageStatus

	Timestamp string `json:"timestamp"`
}

type AttachInput struct {
	HostID          string `json:"hostId"`
	DisableFrontend bool   `json:"disableFrontend"`
}

type SnapshotInput struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type BackupInput struct {
	Name string `json:"name"`
}

type RecurringInput struct {
	Jobs []types.RecurringJob `json:"jobs"`
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

type NodeInput struct {
	NodeID string `json:"nodeId"`
}

type UpdateReplicaCountInput struct {
	ReplicaCount int `json:"replicaCount"`
}

type UpdateDataLocalityInput struct {
	DataLocality string `json:"dataLocality"`
}

type PVCreateInput struct {
	PVName string `json:"pvName"`
	FSType string `json:"fsType"`
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
	Name              string                     `json:"name"`
	Address           string                     `json:"address"`
	AllowScheduling   bool                       `json:"allowScheduling"`
	EvictionRequested bool                       `json:"evictionRequested"`
	Disks             map[string]DiskInfo        `json:"disks"`
	Conditions        map[string]types.Condition `json:"conditions"`
	Tags              []string                   `json:"tags"`
	Region            string                     `json:"region"`
	Zone              string                     `json:"zone"`

	Timestamp string `json:"timestamp"`
}

type DiskInfo struct {
	types.DiskSpec
	types.DiskStatus
}

type DiskUpdateInput struct {
	Disks map[string]types.DiskSpec `json:"disks"`
}

type Event struct {
	client.Resource
	Event     v1.Event `json:"event"`
	EventType string   `json:"eventType"`

	Timestamp string `json:"timestamp"`
}

type SupportBundle struct {
	client.Resource
	NodeID             string              `json:"nodeID"`
	State              manager.BundleState `json:"state"`
	Name               string              `json:"name"`
	ErrorMessage       manager.BundleError `json:"errorMessage"`
	ProgressPercentage int                 `json:"progressPercentage"`
}

type SupportBundleInitateInput struct {
	IssueURL    string `json:"issueURL"`
	Description string `json:"description"`
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

func generateTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

type InstanceManager struct {
	client.Resource
	CurrentState types.InstanceManagerState       `json:"currentState"`
	Image        string                           `json:"image"`
	Name         string                           `json:"name"`
	NodeID       string                           `json:"nodeID"`
	ManagerType  string                           `json:"managerType"`
	Instances    map[string]types.InstanceProcess `json:"instances"`
}

type BackupListOutput struct {
	Data []Backup `json:"data"`
	Type string   `json:"type"`
}

type SnapshotListOutput struct {
	Data []Snapshot `json:"data"`
	Type string     `json:"type"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("attachInput", AttachInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("backup", Backup{})
	schemas.AddType("backupInput", BackupInput{})
	schemas.AddType("backupStatus", BackupStatus{})
	schemas.AddType("restoreStatus", RestoreStatus{})
	schemas.AddType("purgeStatus", PurgeStatus{})
	schemas.AddType("rebuildStatus", RebuildStatus{})
	schemas.AddType("recurringJob", types.RecurringJob{})
	schemas.AddType("replicaRemoveInput", ReplicaRemoveInput{})
	schemas.AddType("salvageInput", SalvageInput{})
	schemas.AddType("activateInput", ActivateInput{})
	schemas.AddType("expandInput", ExpandInput{})
	schemas.AddType("engineUpgradeInput", EngineUpgradeInput{})
	schemas.AddType("replica", Replica{})
	schemas.AddType("controller", Controller{})
	schemas.AddType("diskUpdate", types.DiskSpec{})
	schemas.AddType("nodeInput", NodeInput{})
	schemas.AddType("UpdateReplicaCountInput", UpdateReplicaCountInput{})
	schemas.AddType("UpdateDataLocalityInput", UpdateDataLocalityInput{})
	schemas.AddType("workloadStatus", types.WorkloadStatus{})

	schemas.AddType("PVCreateInput", PVCreateInput{})
	schemas.AddType("PVCCreateInput", PVCCreateInput{})

	schemas.AddType("settingDefinition", types.SettingDefinition{})
	// to avoid duplicate name with built-in type condition
	schemas.AddType("volumeCondition", types.Condition{})
	schemas.AddType("nodeCondition", types.Condition{})
	schemas.AddType("diskCondition", types.Condition{})

	schemas.AddType("event", Event{})
	schemas.AddType("supportBundle", SupportBundle{})
	schemas.AddType("supportBundleInitateInput", SupportBundleInitateInput{})

	schemas.AddType("tag", Tag{})

	schemas.AddType("instanceManager", InstanceManager{})
	schemas.AddType("instanceProcess", types.InstanceProcess{})

	volumeSchema(schemas.AddType("volume", Volume{}))
	snapshotSchema(schemas.AddType("snapshot", Snapshot{}))
	backupVolumeSchema(schemas.AddType("backupVolume", BackupVolume{}))
	settingSchema(schemas.AddType("setting", Setting{}))
	recurringSchema(schemas.AddType("recurringInput", RecurringInput{}))
	engineImageSchema(schemas.AddType("engineImage", EngineImage{}))
	nodeSchema(schemas.AddType("node", Node{}))
	diskSchema(schemas.AddType("diskUpdateInput", DiskUpdateInput{}))
	diskInfoSchema(schemas.AddType("diskInfo", DiskInfo{}))
	kubernetesStatusSchema(schemas.AddType("kubernetesStatus", types.KubernetesStatus{}))
	backupListOutputSchema(schemas.AddType("backupListOutput", BackupListOutput{}))
	snapshotListOutputSchema(schemas.AddType("snapshotListOutput", SnapshotListOutput{}))

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

func recurringSchema(recurring *client.Schema) {
	jobs := recurring.ResourceFields["jobs"]
	jobs.Type = "array[recurringJob]"
	recurring.ResourceFields["jobs"] = jobs
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

		"recurringUpdate": {
			Input: "recurringInput",
		},

		"updateReplicaCount": {
			Input: "UpdateReplicaCountInput",
		},

		"updateDataLocality": {
			Input: "UpdateDataLocalityInput",
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

	volumeNumberOfReplicas := volume.ResourceFields["numberOfReplicas"]
	volumeNumberOfReplicas.Create = true
	volumeNumberOfReplicas.Required = true
	volumeNumberOfReplicas.Default = 2
	volume.ResourceFields["numberOfReplicas"] = volumeNumberOfReplicas

	volumeDataLocality := volume.ResourceFields["dataLocality"]
	volumeDataLocality.Create = true
	volumeDataLocality.Default = types.DataLocalityDisabled
	volume.ResourceFields["dataLocality"] = volumeDataLocality

	volumeStaleReplicaTimeout := volume.ResourceFields["staleReplicaTimeout"]
	volumeStaleReplicaTimeout.Create = true
	volumeStaleReplicaTimeout.Default = 2880
	volume.ResourceFields["staleReplicaTimeout"] = volumeStaleReplicaTimeout

	volumeBaseImage := volume.ResourceFields["baseImage"]
	volumeBaseImage.Create = true
	volume.ResourceFields["baseImage"] = volumeBaseImage

	replicas := volume.ResourceFields["replicas"]
	replicas.Type = "array[replica]"
	volume.ResourceFields["replicas"] = replicas

	recurringJobs := volume.ResourceFields["recurringJobs"]
	recurringJobs.Create = true
	recurringJobs.Default = nil
	recurringJobs.Type = "array[recurringJob]"
	volume.ResourceFields["recurringJobs"] = recurringJobs

	standby := volume.ResourceFields["standby"]
	standby.Create = true
	standby.Default = false
	volume.ResourceFields["standby"] = standby

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
}

func snapshotSchema(snapshot *client.Schema) {
	children := snapshot.ResourceFields["children"]
	children.Type = "map[bool]"
	snapshot.ResourceFields["children"] = children
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

func toSettingResource(setting *longhorn.Setting) *Setting {
	timestamp := generateTimestamp()
	return &Setting{
		Resource: client.Resource{
			Id:    setting.Name,
			Type:  "setting",
			Links: map[string]string{},
		},
		Name:  setting.Name,
		Value: setting.Value,

		Definition: types.SettingDefinitions[types.SettingName(setting.Name)],

		Timestamp: timestamp,
	}
}

func toSettingCollection(settings []*longhorn.Setting) *client.GenericCollection {
	data := []interface{}{}
	for _, setting := range settings {
		data = append(data, toSettingResource(setting))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "setting"}}
}

func toVolumeResource(v *longhorn.Volume, ves []*longhorn.Engine, vrs []*longhorn.Replica, apiContext *api.ApiContext) *Volume {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	var ve *longhorn.Engine
	controllers := []Controller{}
	backups := []BackupStatus{}
	restoreStatus := []RestoreStatus{}
	var purgeStatuses []PurgeStatus
	rebuildStatuses := []RebuildStatus{}
	for _, e := range ves {
		controllers = append(controllers, Controller{
			Instance: Instance{
				Name:                e.Name,
				Running:             e.Status.CurrentState == types.InstanceStateRunning,
				NodeID:              e.Spec.NodeID,
				Address:             e.Status.IP,
				EngineImage:         e.Spec.EngineImage,
				CurrentImage:        e.Status.CurrentImage,
				InstanceManagerName: e.Status.InstanceManagerName,
			},
			Size:                   strconv.FormatInt(e.Status.CurrentSize, 10),
			Endpoint:               e.Status.Endpoint,
			LastRestoredBackup:     e.Status.LastRestoredBackup,
			RequestedBackupRestore: e.Spec.RequestedBackupRestore,
			IsExpanding:            e.Status.IsExpanding,
			LastExpansionError:     e.Status.LastExpansionError,
			LastExpansionFailedAt:  e.Status.LastExpansionFailedAt,
		})
		if e.Spec.NodeID == v.Status.CurrentNodeID {
			ve = e
		}
		backupStatus := e.Status.BackupStatus
		if backupStatus != nil {
			for id, status := range backupStatus {
				backups = append(backups, BackupStatus{
					Resource:  client.Resource{},
					Name:      id,
					Snapshot:  status.SnapshotName,
					Progress:  status.Progress,
					BackupURL: status.BackupURL,
					Error:     status.Error,
					State:     status.State,
					Replica:   datastore.ReplicaAddressToReplicaName(status.ReplicaAddress, vrs),
				})
			}
		}
		rs := e.Status.RestoreStatus
		if rs != nil {
			for replica, status := range rs {
				restoreStatus = append(restoreStatus, RestoreStatus{
					Resource:     client.Resource{},
					Replica:      datastore.ReplicaAddressToReplicaName(replica, vrs),
					IsRestoring:  status.IsRestoring,
					LastRestored: status.LastRestored,
					Progress:     status.Progress,
					Error:        status.Error,
					Filename:     status.Filename,
					State:        status.State,
					BackupURL:    status.BackupURL,
				})
			}
		}
		purgeStatus := e.Status.PurgeStatus
		if purgeStatus != nil {
			for replica, status := range purgeStatus {
				purgeStatuses = append(purgeStatuses, PurgeStatus{
					Resource:  client.Resource{},
					Replica:   datastore.ReplicaAddressToReplicaName(replica, vrs),
					Error:     status.Error,
					IsPurging: status.IsPurging,
					Progress:  status.Progress,
					State:     status.State,
				})
			}
		}
		rebuildStatus := e.Status.RebuildStatus
		if rebuildStatus != nil {
			for replica, status := range rebuildStatus {
				rebuildStatuses = append(rebuildStatuses, RebuildStatus{
					Resource:     client.Resource{},
					Replica:      datastore.ReplicaAddressToReplicaName(replica, vrs),
					Error:        status.Error,
					IsRebuilding: status.IsRebuilding,
					Progress:     status.Progress,
					State:        status.State,
					FromReplica:  datastore.ReplicaAddressToReplicaName(status.FromReplicaAddress, vrs),
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
				Running:             r.Status.CurrentState == types.InstanceStateRunning,
				Address:             r.Status.IP,
				NodeID:              r.Spec.NodeID,
				EngineImage:         r.Spec.EngineImage,
				CurrentImage:        r.Status.CurrentImage,
				InstanceManagerName: r.Status.InstanceManagerName,
			},
			DiskID:   r.Spec.DiskID,
			DataPath: r.Spec.DataPath,
			Mode:     mode,
			FailedAt: r.Spec.FailedAt,
		})
	}

	// The volume is not ready for workloads if:
	//   1. It's auto attached.
	//   2. It fails to schedule replicas during the volume creation,
	//      in which case scheduling failure will happen when the volume is detached.
	//      In other cases, scheduling failure only happens when the volume is attached.
	//   3. It's faulted.
	ready := true
	scheduledCondition := types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeScheduled)
	if (v.Spec.NodeID == "" && v.Status.State != types.VolumeStateDetached) ||
		(v.Status.State == types.VolumeStateDetached && scheduledCondition.Status != types.ConditionStatusTrue) ||
		v.Status.Robustness == types.VolumeRobustnessFaulted {
		ready = false
	}

	r := &Volume{
		Resource: client.Resource{
			Id:      v.Name,
			Type:    "volume",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:                v.Name,
		Size:                strconv.FormatInt(v.Spec.Size, 10),
		Frontend:            v.Spec.Frontend,
		DisableFrontend:     v.Spec.DisableFrontend,
		FromBackup:          v.Spec.FromBackup,
		NumberOfReplicas:    v.Spec.NumberOfReplicas,
		DataLocality:        v.Spec.DataLocality,
		RecurringJobs:       v.Spec.RecurringJobs,
		StaleReplicaTimeout: v.Spec.StaleReplicaTimeout,
		Created:             v.CreationTimestamp.String(),
		EngineImage:         v.Spec.EngineImage,
		BaseImage:           v.Spec.BaseImage,
		Standby:             v.Spec.Standby,
		DiskSelector:        v.Spec.DiskSelector,
		NodeSelector:        v.Spec.NodeSelector,

		State:           v.Status.State,
		Robustness:      v.Status.Robustness,
		CurrentImage:    v.Status.CurrentImage,
		LastBackup:      v.Status.LastBackup,
		LastBackupAt:    v.Status.LastBackupAt,
		RestoreRequired: v.Status.RestoreRequired,
		Ready:           ready,

		Conditions:       v.Status.Conditions,
		KubernetesStatus: v.Status.KubernetesStatus,

		Controllers:   controllers,
		Replicas:      replicas,
		BackupStatus:  backups,
		RestoreStatus: restoreStatus,
		PurgeStatus:   purgeStatuses,
		RebuildStatus: rebuildStatuses,

		Timestamp: timestamp,
	}

	actions := map[string]struct{}{}

	if v.Status.Robustness == types.VolumeRobustnessFaulted {
		actions["salvage"] = struct{}{}
	} else {
		switch v.Status.State {
		case types.VolumeStateDetached:
			actions["attach"] = struct{}{}
			actions["recurringUpdate"] = struct{}{}
			actions["activate"] = struct{}{}
			actions["expand"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
		case types.VolumeStateAttaching:
			actions["detach"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
		case types.VolumeStateAttached:
			actions["detach"] = struct{}{}
			actions["activate"] = struct{}{}
			actions["snapshotPurge"] = struct{}{}
			actions["snapshotCreate"] = struct{}{}
			actions["snapshotList"] = struct{}{}
			actions["snapshotGet"] = struct{}{}
			actions["snapshotDelete"] = struct{}{}
			actions["snapshotRevert"] = struct{}{}
			actions["snapshotBackup"] = struct{}{}
			actions["recurringUpdate"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["updateReplicaCount"] = struct{}{}
			actions["updateDataLocality"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
			actions["cancelExpansion"] = struct{}{}
		}
	}

	for action := range actions {
		r.Actions[action] = apiContext.UrlBuilder.ActionLink(r.Resource, action)
	}

	return r
}

func toSnapshotResource(s *types.Snapshot) *Snapshot {
	if s == nil {
		logrus.Warn("weird: nil snapshot")
		return nil
	}
	return &Snapshot{
		Resource: client.Resource{
			Id:   s.Name,
			Type: "snapshot",
		},
		Snapshot: *s,
	}
}

func toSnapshotCollection(ss map[string]*types.Snapshot) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range ss {
		data = append(data, toSnapshotResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "snapshot"}}
}

func toBackupVolumeResource(bv *engineapi.BackupVolume, apiContext *api.ApiContext) *BackupVolume {
	if bv == nil {
		logrus.Warnf("weird: nil backupVolume")
		return nil
	}
	b := &BackupVolume{
		Resource: client.Resource{
			Id:    bv.Name,
			Type:  "backupVolume",
			Links: map[string]string{},
		},
		BackupVolume: *bv,
	}
	b.Actions = map[string]string{
		"backupList":   apiContext.UrlBuilder.ActionLink(b.Resource, "backupList"),
		"backupGet":    apiContext.UrlBuilder.ActionLink(b.Resource, "backupGet"),
		"backupDelete": apiContext.UrlBuilder.ActionLink(b.Resource, "backupDelete"),
	}
	return b
}

func toBackupVolumeCollection(bv map[string]*engineapi.BackupVolume, apiContext *api.ApiContext) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range bv {
		data = append(data, toBackupVolumeResource(v, apiContext))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backupVolume"}}
}

func toBackupResource(b *engineapi.Backup) *Backup {
	if b == nil {
		logrus.Warnf("weird: nil backup")
		return nil
	}
	return &Backup{
		Resource: client.Resource{
			Id:    b.Name,
			Type:  "backup",
			Links: map[string]string{},
		},
		Backup: *b,
	}
}

func toBackupCollection(bs []*engineapi.Backup) *client.GenericCollection {
	data := []interface{}{}
	for _, v := range bs {
		data = append(data, toBackupResource(v))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "backup"}}
}

func toEngineImageResource(ei *longhorn.EngineImage, isDefault bool) *EngineImage {
	timestamp := generateTimestamp()
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

		Timestamp: timestamp,
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
	timestamp := generateTimestamp()
	n := &Node{
		Resource: client.Resource{
			Id:      node.Name,
			Type:    "node",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:              node.Name,
		Address:           address,
		AllowScheduling:   node.Spec.AllowScheduling,
		EvictionRequested: node.Spec.EvictionRequested,
		Conditions:        node.Status.Conditions,
		Tags:              node.Spec.Tags,
		Region:            node.Status.Region,
		Zone:              node.Status.Zone,

		Timestamp: timestamp,
	}

	disks := map[string]DiskInfo{}
	for name, disk := range node.Spec.Disks {
		di := DiskInfo{
			DiskSpec: disk,
		}
		if node.Status.DiskStatus != nil && node.Status.DiskStatus[name] != nil {
			di.DiskStatus = *node.Status.DiskStatus[name]
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

func toEventResource(event v1.Event) *Event {
	timestamp := generateTimestamp()
	e := &Event{
		Resource: client.Resource{
			Id:    event.Name,
			Type:  "event",
			Links: map[string]string{},
		},
		Event:     event,
		EventType: event.Type,

		Timestamp: timestamp,
	}
	return e
}

func toEventCollection(eventList *v1.EventList) *client.GenericCollection {
	data := []interface{}{}
	for _, event := range eventList.Items {
		data = append(data, toEventResource(event))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "event"}}
}

//Support Bundle Resource
func toSupportBundleResource(nodeID string, sb *manager.SupportBundle) *SupportBundle {
	return &SupportBundle{
		Resource: client.Resource{
			Id:   nodeID,
			Type: "supportbundle",
		},
		NodeID:             nodeID,
		State:              sb.State,
		Name:               sb.Name,
		ErrorMessage:       sb.Error,
		ProgressPercentage: sb.ProgressPercentage,
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
		CurrentState: im.Status.CurrentState,
		Image:        im.Spec.Image,
		Name:         im.Name,
		NodeID:       im.Spec.NodeID,
		ManagerType:  string(im.Spec.Type),
		Instances:    im.Status.Instances,
	}
}

func toInstanceManagerCollection(instanceManagers map[string]*longhorn.InstanceManager) *client.GenericCollection {
	var data []interface{}
	for _, im := range instanceManagers {
		data = append(data, toInstanceManagerResource(im))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "instanceManager"}}
}
