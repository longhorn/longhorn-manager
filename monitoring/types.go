package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	longhornName = "longhorn"

	subsystemVolume    = "volume"
	subsystemNodeStats = "node"

	nodeLabel            = "node"
	volumeLabel          = "volume"
	conditionLabel       = "condition"
	conditionReasonLabel = "condition_reason"
)

type metricInfo struct {
	Desc *prometheus.Desc
	Type prometheus.ValueType
}
