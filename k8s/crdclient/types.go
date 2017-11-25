package crdclient

import (
	"github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn"
	types "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

const (
	KeyVolume     string = "volume"
	KeyNode       string = "node"
	KeyReplica    string = "replica"
	KeySetting    string = "setting"
	KeyController string = "controller"
)

type CRDInfo struct {
	Plural    string
	FullName  string
	ShortName string
	Obj       interface{}
}

var CRDInfoMap = map[string]CRDInfo{
	KeyVolume: {"volumes", "volumes" + "." + longhorn.GroupName,
		"lhv", types.Volume{}},
	KeyNode: {"nodes", "nodes" + "." + longhorn.GroupName,
		"lhn", types.Node{}},
	KeyController: {"controllers", "controllers" + "." + longhorn.GroupName,
		"lhc", types.Controller{}},
	KeyReplica: {"replicas", "replicas" + "." + longhorn.GroupName,
		"lhr", types.Replica{}},
	KeySetting: {"settings", "settings" + "." + longhorn.GroupName,
		"lhs", types.Setting{}},
}
