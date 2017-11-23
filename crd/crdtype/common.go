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
	KeyVolume: Crdtype{"crdvolumes", "crdvolumes" + "." + CRDGroup,
		"cv", CrdVolume{}},
	KeyNode: Crdtype{"crdnodes", "crdnodes" + "." + CRDGroup,
		"cnc", CrdNode{}},
	KeyReplica: Crdtype{"crdreplicas", "crdreplicas" + "." + CRDGroup,
		"cr", CrdReplica{}},
	KeySetting: Crdtype{"crdsettings", "crdsettings" + "." + CRDGroup,
		"crs", CrdSetting{}},
	KeyController: Crdtype{"crdcontrollers", "crdcontrollers" + "." + CRDGroup,
		"cc", CrdController{}},
}
