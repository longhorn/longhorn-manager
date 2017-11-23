package crdtype

const (
	CRDVersion string = "v1"
	CRDGroup   string = "longhorn.rancher.io"

	KeyVolume     string = "volume"
	KeyNode       string = "node"
	KeyReplica    string = "replica"
	KeySetting    string = "setting"
	KeyController string = "controller"
)

// +k8s:deepcopy-gen=false
type Crdtype struct {
	CrdPlural    string
	CrdFullName  string
	CrdShortName string
	CrdObj       interface{}
}

var CrdMap = map[string]Crdtype{
	KeyVolume: {"volumes", "volumes" + "." + CRDGroup,
		"cv", Volume{}},
	KeyNode: {"nodes", "nodes" + "." + CRDGroup,
		"cnc", Node{}},
	KeyReplica: {"replicas", "replicas" + "." + CRDGroup,
		"cr", Replica{}},
	KeySetting: {"settings", "settings" + "." + CRDGroup,
		"crs", Setting{}},
	KeyController: {"controllers", "controllers" + "." + CRDGroup,
		"cc", Controller{}},
}
