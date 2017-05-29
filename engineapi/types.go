package engineapi

import (
	"fmt"
	"strings"
)

type ReplicaMode string

const (
	ControllerDefaultPort = "9501"
	ReplicaDefaultPort    = "9503"

	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)

type Replica struct {
	URL  string
	Mode ReplicaMode
}

type Controller struct {
	URL    string
	NodeID string
}

type EngineClient interface {
	Name() string
	Endpoint() string

	ReplicaList() (map[string]*Replica, error)
	ReplicaAdd(url string) error
	ReplicaRemove(url string) error
}

type EngineClientRequest struct {
	VolumeName    string
	ControllerURL string
}

type EngineClientCollection interface {
	NewEngineClient(request *EngineClientRequest) (EngineClient, error)
}

type Volume struct {
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	Endpoint     string `json:"endpoint"`
}

func GetControllerDefaultURL(ip string) string {
	return "http://" + ip + ":" + ControllerDefaultPort
}

func GetReplicaDefaultURL(ip string) string {
	return "tcp://" + ip + ":" + ReplicaDefaultPort
}

func GetIPFromURL(url string) string {
	// tcp, \/\/<address>, 9502
	return strings.TrimPrefix(strings.Split(url, ":")[1], "//")
}

func ValidateReplicaURL(url string) error {
	if !strings.HasPrefix(url, "tcp://") {
		return fmt.Errorf("invalid replica url %v", url)
	}
	return nil
}
