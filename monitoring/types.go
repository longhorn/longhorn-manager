package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	longhornName = "longhorn"

	subsystemVolume          = "volume"
	subsystemNode            = "node"
	subsystemInstanceManager = "instance_manager"
	subsystemManager         = "manager"

	nodeLabel            = "node"
	volumeLabel          = "volume"
	conditionLabel       = "condition"
	conditionReasonLabel = "condition_reason"
	instanceManagerLabel = "instance_manager"
	instanceManagerType  = "instance_manager_type"
	managerLabel         = "manager"
)

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}
