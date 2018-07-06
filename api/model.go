package api

import (
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

type Volume struct {
	client.Resource

	Name                string                 `json:"name"`
	Size                string                 `json:"size"`
	Frontend            types.VolumeFrontend   `json:"frontend"`
	FromBackup          string                 `json:"fromBackup"`
	NumberOfReplicas    int                    `json:"numberOfReplicas"`
	StaleReplicaTimeout int                    `json:"staleReplicaTimeout"`
	State               types.VolumeState      `json:"state"`
	Robustness          types.VolumeRobustness `json:"robustness"`
	EngineImage         string                 `json:"engineImage"`
	CurrentImage        string                 `json:"currentImage"`
	Endpoint            string                 `json:"endpoint,omitemtpy"`
	Created             string                 `json:"created,omitemtpy"`

	RecurringJobs []types.RecurringJob `json:"recurringJobs"`

	Replicas   []Replica   `json:"replicas"`
	Controller *Controller `json:"controller"`
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
	Name  string `json:"name"`
	Value string `json:"value"`
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
}

type Replica struct {
	Instance

	Mode     string `json:"mode"`
	FailedAt string `json:"failedAt"`
}

type EngineImage struct {
	client.Resource

	Name    string `json:"name"`
	Image   string `json:"image"`
	Default bool   `json:"default"`
	types.EngineImageStatus
}

type AttachInput struct {
	HostID string `json:"hostId"`
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

type Node struct {
	client.Resource
	Name            string          `json:"name"`
	Address         string          `json:"address"`
	AllowScheduling bool            `json:"allowScheduling"`
	State           types.NodeState `json:"state"`
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
	schemas.AddType("recurringJob", types.RecurringJob{})
	schemas.AddType("replicaRemoveInput", ReplicaRemoveInput{})
	schemas.AddType("salvageInput", SalvageInput{})
	schemas.AddType("engineUpgradeInput", EngineUpgradeInput{})
	schemas.AddType("replica", Replica{})
	schemas.AddType("controller", Controller{})
	schemas.AddType("node", Node{})

	volumeSchema(schemas.AddType("volume", Volume{}))
	backupVolumeSchema(schemas.AddType("backupVolume", BackupVolume{}))
	settingSchema(schemas.AddType("setting", Setting{}))
	recurringSchema(schemas.AddType("recurringInput", RecurringInput{}))
	engineImageSchema(schemas.AddType("engineImage", EngineImage{}))
	nodeSchema(schemas.AddType("node", Node{}))

	return schemas
}

func nodeSchema(node *client.Schema) {
	node.CollectionMethods = []string{"GET"}
	node.ResourceMethods = []string{"GET", "PUT"}

	allowScheduling := node.ResourceFields["allowScheduling"]
	allowScheduling.Required = true
	allowScheduling.Unique = false
	node.ResourceFields["allowScheduling"] = allowScheduling
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

func backupVolumeSchema(backupVolume *client.Schema) {
	backupVolume.CollectionMethods = []string{"GET"}
	backupVolume.ResourceMethods = []string{"GET"}
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

		"jobList": {},

		"replicaRemove": {
			Input:  "replicaRemoveInput",
			Output: "volume",
		},

		"engineUpgrade": {
			Input: "engineUpgradeInput",
		},
	}
	volume.ResourceFields["controller"] = client.Field{
		Type:     "controller",
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
	volumeFrontend.Required = true
	volumeFrontend.Default = "blockdev"
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

	replicas := volume.ResourceFields["replicas"]
	replicas.Type = "array[replica]"
	volume.ResourceFields["replicas"] = replicas

	recurringJobs := volume.ResourceFields["recurringJobs"]
	recurringJobs.Type = "array[recurringJob]"
	volume.ResourceFields["recurringJobs"] = recurringJobs
}

func toSettingResource(name, value string) *Setting {
	return &Setting{
		Resource: client.Resource{
			Id:   name,
			Type: "setting",
		},
		Name:  name,
		Value: value,
	}
}

func toSettingCollection(settings *types.SettingsInfo) *client.GenericCollection {
	data := []interface{}{
		toSettingResource(types.SettingBackupTarget, settings.BackupTarget),
		toSettingResource(types.SettingDefaultEngineImage, settings.DefaultEngineImage),
		toSettingResource(types.SettingBackupTargetCredentialSecret, settings.BackupTargetCredentialSecret),
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "setting"}}
}

func toVolumeResource(v *longhorn.Volume, ve *longhorn.Engine, vrs map[string]*longhorn.Replica, apiContext *api.ApiContext) *Volume {
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
			Mode:     mode,
			FailedAt: r.Spec.FailedAt,
		})
	}

	var controller *Controller
	if ve != nil {
		controller = &Controller{Instance{
			Name:         ve.Name,
			Running:      ve.Status.CurrentState == types.InstanceStateRunning,
			NodeID:       ve.Spec.NodeID,
			Address:      ve.Status.IP,
			EngineImage:  ve.Spec.EngineImage,
			CurrentImage: ve.Status.CurrentImage,
		}}
	}

	endpoint := v.Status.Endpoint
	// make it iscsi endpoint with the ip
	if endpoint != "" && v.Spec.Frontend == types.VolumeFrontendISCSI {
		if ve != nil {
			// it will looks like this in the end
			// iscsi://10.42.0.12:3260/iqn.2014-09.com.rancher:vol-name/1
			endpoint = "iscsi://" + ve.Status.IP + ":" + engineapi.DefaultISCSIPort + "/" + endpoint + "/" + engineapi.DefaultISCSILUN
		} else {
			// engine is not ready, don't show endpoint
			endpoint = ""
		}
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
		FromBackup:          v.Spec.FromBackup,
		NumberOfReplicas:    v.Spec.NumberOfReplicas,
		State:               v.Status.State,
		Robustness:          v.Status.Robustness,
		RecurringJobs:       v.Spec.RecurringJobs,
		StaleReplicaTimeout: v.Spec.StaleReplicaTimeout,
		Endpoint:            endpoint,
		Created:             v.ObjectMeta.CreationTimestamp.String(),
		EngineImage:         v.Spec.EngineImage,
		CurrentImage:        v.Status.CurrentImage,

		Controller: controller,
		Replicas:   replicas,
	}

	actions := map[string]struct{}{}

	if v.Status.Robustness == types.VolumeRobustnessFaulted {
		actions["salvage"] = struct{}{}
	} else {
		switch v.Status.State {
		case types.VolumeStateDetached:
			actions["attach"] = struct{}{}
			actions["recurringUpdate"] = struct{}{}
			actions["replicaRemove"] = struct{}{}
			actions["engineUpgrade"] = struct{}{}
		case types.VolumeStateAttaching:
			actions["detach"] = struct{}{}
		case types.VolumeStateAttached:
			actions["detach"] = struct{}{}
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

func toBackupVolumeCollection(bv []*engineapi.BackupVolume, apiContext *api.ApiContext) *client.GenericCollection {
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

func toEngineImageCollection(eis map[string]*longhorn.EngineImage, defaultImage string) *client.GenericCollection {
	data := []interface{}{}
	for _, ei := range eis {
		isDefault := (ei.Spec.Image == defaultImage)
		data = append(data, toEngineImageResource(ei, isDefault))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "engineImage"}}
}

type Server struct {
	m   *manager.VolumeManager
	fwd *Fwd
}

func NewServer(m *manager.VolumeManager) *Server {
	s := &Server{
		m:   m,
		fwd: NewFwd(m),
	}
	return s
}

func toNodeResource(node *longhorn.Node, address string) *Node {
	n := &Node{
		Resource: client.Resource{
			Id:    node.Name,
			Type:  "node",
			Links: map[string]string{},
		},
		Name:            node.Name,
		Address:         address,
		AllowScheduling: node.Spec.AllowScheduling,
		State:           node.Status.State,
	}

	return n
}

func toNodeCollection(nodeList []*longhorn.Node, nodeIPMap map[string]string) *client.GenericCollection {
	data := []interface{}{}
	for _, node := range nodeList {
		data = append(data, toNodeResource(node, nodeIPMap[node.Name]))
	}
	return &client.GenericCollection{Data: data, Collection: client.Collection{ResourceType: "node"}}
}
