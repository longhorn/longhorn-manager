package types

const (
	DefaultAPIPort          = 9500
	DefaultManagerPort      = 9507
	DefaultOrchestratorPort = 9508
)

type ReplicaMode string

const (
	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")
)
