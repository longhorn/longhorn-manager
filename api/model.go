package api

import (
	"strconv"
	"strings"
	"time"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

type Volume struct {
	client.Resource

	Name                       string                 `json:"name"`
	Size                       string                 `json:"size"`
	Frontend                   types.VolumeFrontend   `json:"frontend"`
	DisableFrontend            bool                   `json:"disableFrontend"`
	FromBackup                 string                 `json:"fromBackup"`
	NumberOfReplicas           int                    `json:"numberOfReplicas"`
	StaleReplicaTimeout        int                    `json:"staleReplicaTimeout"`
	State                      types.VolumeState      `json:"state"`
	Robustness                 types.VolumeRobustness `json:"robustness"`
	EngineImage                string                 `json:"engineImage"`
	CurrentImage               string                 `json:"currentImage"`
	BaseImage                  string                 `json:"baseImage"`
	Created                    string                 `json:"created"`
	MigrationNodeID            string                 `json:"migrationNodeID"`
	LastBackup                 string                 `json:"lastBackup"`
	LastBackupAt               string                 `json:"lastBackupAt"`
	Standby                    bool                   `json:"standby"`
	InitialRestorationRequired bool                   `json:"initialRestorationRequired"`
	DiskSelector               []string               `json:"diskSelector"`
	NodeSelector               []string               `json:"nodeSelector"`

	RecurringJobs    []types.RecurringJob                          `json:"recurringJobs"`
	Conditions       map[types.VolumeConditionType]types.Condition `json:"conditions"`
	KubernetesStatus types.KubernetesStatus                        `json:"kubernetesStatus"`

	Replicas      []Replica       `json:"replicas"`
	Controllers   []Controller    `json:"controllers"`
	BackupStatus  []BackupStatus  `json:"backupStatus"`
	RestoreStatus []RestoreStatus `json:"restoreStatus"`

	Timestamp string `json:"timestamp"`
}

type Snapshot struct {
	client.Resource
	engineapi.Snapshot
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
	Name         string `json:"name"`
	NodeID       string `json:"hostId"`
	Address      string `json:"address"`
	Running      bool   `json:"running"`
	EngineImage  string `json:"engineImage"`
	CurrentImage string `json:"currentImage"`
}

type Controller struct {
	Instance
	Endpoint               string `json:"endpoint"`
	LastRestoredBackup     string `json:"lastRestoredBackup"`
	RequestedBackupRestore string `json:"requestedBackupRestore"`
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

type PVCreateInput struct {
	PVName string `json:"pvName"`
}

type PVCCreateInput struct {
	Namespace string `json:"namespace"`
	PVCName   string `json:"pvcName"`
}

type ActivateInput struct {
	Frontend string `json:"frontend"`
}

type Node struct {
	client.Resource
	Name            string                                      `json:"name"`
	Address         string                                      `json:"address"`
	AllowScheduling bool                                        `json:"allowScheduling"`
	Disks           map[string]DiskInfo                         `json:"disks"`
	Conditions      map[types.NodeConditionType]types.Condition `json:"conditions"`
	Tags            []string                                    `json:"tags"`

	Timestamp string `json:"timestamp"`
}

type DiskInfo struct {
	types.DiskSpec
	types.DiskStatus
}

type DiskUpdateInput struct {
	Disks []types.DiskSpec `json:"disks"`
}

type Event struct {
	client.Resource
	v1.Event
	EventType string `json:"eventType"`

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

func generateTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

type InstanceManager struct {
	client.Resource
	CurrentState types.InstanceManagerState `json:"currentState"`
	EngineImage  string                     `json:"engineImage"`
	Name         string                     `json:"name"`
	NodeID       string                     `json:"nodeID"`
}

func NewSchema() *client.Schemas {
	schemas := &client.Schemas{}

	schemas.AddType("apiVersion", client.Resource{})
	schemas.AddType("schema", client.Schema{})
	schemas.AddType("error", client.ServerApiError{})
	schemas.AddType("snapshot", Snapshot{})
	schemas.AddType("attachInput", AttachInput{})
	schemas.AddType("snapshotInput", SnapshotInput{})
	schemas.AddType("backup", Backup{})
	schemas.AddType("backupInput", BackupInput{})
	schemas.AddType("backupStatus", BackupStatus{})
	schemas.AddType("restoreStatus", RestoreStatus{})
	schemas.AddType("recurringJob", types.RecurringJob{})
	schemas.AddType("replicaRemoveInput", ReplicaRemoveInput{})
	schemas.AddType("salvageInput", SalvageInput{})
	schemas.AddType("activateInput", ActivateInput{})
	schemas.AddType("engineUpgradeInput", EngineUpgradeInput{})
	schemas.AddType("replica", Replica{})
	schemas.AddType("controller", Controller{})
	schemas.AddType("diskUpdate", types.DiskSpec{})
	schemas.AddType("nodeInput", NodeInput{})
	schemas.AddType("UpdateReplicaCountInput", UpdateReplicaCountInput{})
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

	schemas.AddType("instanceManager", InstanceManager{})
	schemas.AddType("tag", Tag{})

	volumeSchema(schemas.AddType("volume", Volume{}))
	backupVolumeSchema(schemas.AddType("backupVolume", BackupVolume{}))
	settingSchema(schemas.AddType("setting", Setting{}))
	recurringSchema(schemas.AddType("recurringInput", RecurringInput{}))
	engineImageSchema(schemas.AddType("engineImage", EngineImage{}))
	nodeSchema(schemas.AddType("node", Node{}))
	diskSchema(schemas.AddType("diskUpdateInput", DiskUpdateInput{}))
	diskInfoSchema(schemas.AddType("diskInfo", DiskInfo{}))
	kubernetesStatusSchema(schemas.AddType("kubernetesStatus", types.KubernetesStatus{}))

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
		"backupList": {},
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
			Input: "activateInput",
		},

		"snapshotPurge": {},
		"snapshotCreate": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotGet": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotList": {},
		"snapshotDelete": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotRevert": {
			Input:  "snapshotInput",
			Output: "snapshot",
		},
		"snapshotBackup": {
			Input: "snapshotInput",
		},

		"recurringUpdate": {
			Input: "recurringInput",
		},

		"updateReplicaCount": {
			Input: "UpdateReplicaCountInput",
		},

		"pvCreate": {
			Input: "PVCreateInput",
		},
		"pvcCreate": {
			Input: "PVCCreateInput",
		},

		"jobList": {},

		"replicaRemove": {
			Input:  "replicaRemoveInput",
			Output: "volume",
		},

		"engineUpgrade": {
			Input: "engineUpgradeInput",
		},

		"migrationStart": {
			Input: "nodeInput",
		},
		"migrationConfirm":  {},
		"migrationRollback": {},
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

	volumeStaleReplicaTimeout := volume.ResourceFields["staleReplicaTimeout"]
	volumeStaleReplicaTimeout.Create = true
	volumeStaleReplicaTimeout.Default = 20
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

func getReplicaName(address string, vrs []*longhorn.Replica, volumeName string) string {
	for _, r := range vrs {
		inputIP := strings.Split(strings.TrimPrefix(address, "tcp://"), ":")[0]
		if inputIP == r.Status.IP {
			return r.Name
		}
	}
	logrus.Warnf("replica address[%v] received from engine restoreStatus for volume[%v] is not found",
		address, volumeName)
	return address
}

func toVolumeResource(v *longhorn.Volume, ves []*longhorn.Engine, vrs []*longhorn.Replica, apiContext *api.ApiContext) *Volume {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	var ve *longhorn.Engine
	controllers := []Controller{}
	backups := []BackupStatus{}
	restoreStatus := []RestoreStatus{}
	for _, e := range ves {
		controllers = append(controllers, Controller{
			Instance: Instance{
				Name:         e.Name,
				Running:      e.Status.CurrentState == types.InstanceStateRunning,
				NodeID:       e.Spec.NodeID,
				Address:      e.Status.IP,
				EngineImage:  e.Spec.EngineImage,
				CurrentImage: e.Status.CurrentImage,
			},
			Endpoint:               e.Status.Endpoint,
			LastRestoredBackup:     e.Status.LastRestoredBackup,
			RequestedBackupRestore: e.Spec.RequestedBackupRestore,
		})
		if e.Spec.NodeID == v.Spec.NodeID {
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
				})
			}
		}
		rs := e.Status.RestoreStatus
		if rs != nil {
			for replica, status := range rs {
				restoreStatus = append(restoreStatus, RestoreStatus{
					Resource:     client.Resource{},
					Replica:      getReplicaName(replica, vrs, v.Name),
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
	}

	replicas := []Replica{}
	for _, r := range vrs {
		mode := ""
		if ve != nil && ve.Status.ReplicaModeMap != nil {
			mode = string(ve.Status.ReplicaModeMap[r.Name])
		}
		replicas = append(replicas, Replica{
			Instance: Instance{
				Name:         r.Name,
				Running:      r.Status.CurrentState == types.InstanceStateRunning,
				Address:      r.Status.IP,
				NodeID:       r.Spec.NodeID,
				EngineImage:  r.Spec.EngineImage,
				CurrentImage: r.Status.CurrentImage,
			},
			DiskID:   r.Spec.DiskID,
			DataPath: r.Spec.DataPath,
			Mode:     mode,
			FailedAt: r.Spec.FailedAt,
		})
	}

	r := &Volume{
		Resource: client.Resource{
			Id:      v.Name,
			Type:    "volume",
			Actions: map[string]string{},
			Links:   map[string]string{},
		},
		Name:                       v.Name,
		Size:                       strconv.FormatInt(v.Spec.Size, 10),
		Frontend:                   v.Spec.Frontend,
		DisableFrontend:            v.Spec.DisableFrontend,
		FromBackup:                 v.Spec.FromBackup,
		NumberOfReplicas:           v.Spec.NumberOfReplicas,
		State:                      v.Status.State,
		Robustness:                 v.Status.Robustness,
		RecurringJobs:              v.Spec.RecurringJobs,
		StaleReplicaTimeout:        v.Spec.StaleReplicaTimeout,
		Created:                    v.CreationTimestamp.String(),
		EngineImage:                v.Spec.EngineImage,
		CurrentImage:               v.Status.CurrentImage,
		BaseImage:                  v.Spec.BaseImage,
		MigrationNodeID:            v.Spec.MigrationNodeID,
		LastBackup:                 v.Status.LastBackup,
		LastBackupAt:               v.Status.LastBackupAt,
		Standby:                    v.Spec.Standby,
		InitialRestorationRequired: v.Spec.InitialRestorationRequired,
		DiskSelector:               v.Spec.DiskSelector,
		NodeSelector:               v.Spec.NodeSelector,

		Conditions:       v.Status.Conditions,
		KubernetesStatus: v.Status.KubernetesStatus,

		Controllers:   controllers,
		Replicas:      replicas,
		BackupStatus:  backups,
		RestoreStatus: restoreStatus,

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
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
		case types.VolumeStateAttaching:
			actions["detach"] = struct{}{}
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
			actions["migrationStart"] = struct{}{}
			actions["migrationConfirm"] = struct{}{}
			actions["migrationRollback"] = struct{}{}
			actions["updateReplicaCount"] = struct{}{}
			actions["pvCreate"] = struct{}{}
			actions["pvcCreate"] = struct{}{}
		}
	}

	for action := range actions {
		r.Actions[action] = apiContext.UrlBuilder.ActionLink(r.Resource, action)
	}

	return r
}

func toSnapshotResource(s *engineapi.Snapshot) *Snapshot {
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

func toSnapshotCollection(ss map[string]*engineapi.Snapshot) *client.GenericCollection {
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
		Name:            node.Name,
		Address:         address,
		AllowScheduling: node.Spec.AllowScheduling,
		Conditions:      node.Status.Conditions,
		Tags:            node.Spec.Tags,

		Timestamp: timestamp,
	}

	disks := map[string]DiskInfo{}
	for name, disk := range node.Spec.Disks {
		di := DiskInfo{
			disk,
			node.Status.DiskStatus[name],
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
		EngineImage:  im.Spec.EngineImage,
		Name:         im.Name,
		NodeID:       im.Spec.NodeID,
	}
}

func toInstanceManagerCollection(instanceManagers map[string]*longhorn.InstanceManager) *client.GenericCollection {
	var data []interface{}
	for _, im := range instanceManagers {
		data = append(data, toInstanceManagerResource(im))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "instanceManager"}}
}
