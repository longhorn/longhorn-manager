package v1beta1

type ConditionStatus string

const (
	ConditionStatusTrue    ConditionStatus = "True"
	ConditionStatusFalse   ConditionStatus = "False"
	ConditionStatusUnknown ConditionStatus = "Unknown"
)

type Condition struct {
	// +optional
	Type string `json:"type"`
	// +optional
	Status ConditionStatus `json:"status"`
	// +optional
	LastProbeTime string `json:"lastProbeTime"`
	// +optional
	LastTransitionTime string `json:"lastTransitionTime"`
	// +optional
	Reason string `json:"reason"`
	// +optional
	Message string `json:"message"`
}
