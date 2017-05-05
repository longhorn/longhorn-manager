package types

type VolumeCreateRequest struct {
	Name                string `json:"name"`
	Size                string `json:"size"`
	BaseImage           string `json:"baseImage"`
	FromBackup          string `json:"fromBackup"`
	NumberOfReplicas    int    `json:"numberOfReplicas"`
	StaleReplicaTimeout int    `json:"staleReplicaTimeout"`
}

type VolumeAttachRequest struct {
	Name   string `json:"name"`
	HostID string `json:"hostId"`
}

type VolumeDetachRequest struct {
	Name string `json:"name"`
}

type VolumeDeleteRequest struct {
	Name string `json:"name"`
}

type VolumeSalvageRequest struct {
	Name                string   `json:"name"`
	SalvageReplicaNames []string `json:"salvageReplicaNames"`
}
