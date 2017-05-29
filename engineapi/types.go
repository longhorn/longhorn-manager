package engineapi

import (
	"strings"
)

type ReplicaMode string

const (
	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)

type Replica struct {
	Address string
	Mode    ReplicaMode
}

type Controller struct {
	Address string
	NodeID  string
}

type EngineClient interface {
	Name() string
	Endpoint() string
	GetReplicaStates() (map[string]*Replica, error)
	AddReplica(addr string) error
	RemoveReplica(addr string) error
}

type EngineClientRequest struct {
	VolumeName     string
	ControllerAddr string
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}

type Volume struct {
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	Endpoint     string `json:"endpoint"`
}

func GetControllerURL(ip string) string {
	return "http://" + ip + ":9501"
}

func GetReplicaURL(ip string) string {
	return "tcp://" + ip + ":9502"
}

func GetIPFromURL(url string) string {
	// tcp, \/\/<address>, 9502
	return strings.TrimPrefix(strings.Split(url, ":")[1], "//")
}
