package types

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	"github.com/longhorn/longhorn-manager/util"
)

// GetCondition returns a copy of conditions[conditionType], and automatically fill the unknown condition
func GetCondition(conditions map[string]Condition, conditionType string) Condition {
	condition, exists := conditions[conditionType]
	if !exists {
		condition = getUnknownCondition(conditionType)
	}
	return condition
}

func getUnknownCondition(conditionType string) Condition {
	condition := Condition{
		Type:   conditionType,
		Status: ConditionStatusUnknown,
	}
	return condition
}

func SetConditionAndRecord(conditions map[string]Condition, conditionType string, conditionValue ConditionStatus,
	reason, message string, eventRecorder record.EventRecorder, obj runtime.Object, eventtype string) {

	condition := GetCondition(conditions, conditionType)
	if condition.Status != conditionValue {
		eventRecorder.Event(obj, eventtype, conditionType, message)
	}
	SetCondition(conditions, conditionType, conditionValue, reason, message)
}

func SetCondition(conditions map[string]Condition, conditionType string, conditionValue ConditionStatus, reason, message string) {
	condition := GetCondition(conditions, conditionType)
	if condition.Status != conditionValue {
		condition.LastTransitionTime = util.Now()
	}
	condition.Status = conditionValue
	condition.Reason = reason
	condition.Message = message
	conditions[conditionType] = condition
}
