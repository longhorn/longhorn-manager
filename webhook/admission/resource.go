package admission

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
)

type Resource struct {
	Name           string
	Scope          admissionregv1.ScopeType
	APIGroup       string
	APIVersion     string
	ObjectType     runtime.Object
	OperationTypes []admissionregv1.OperationType
}

func (r Resource) Validate() error {
	if r.Name == "" {
		return errUndefined("Name")
	}
	if r.Scope == "" {
		return errUndefined("Scope")
	}
	if r.APIVersion == "" {
		return errUndefined("APIVersion")
	}
	if r.ObjectType == nil {
		return errUndefined("ObjectType")
	}
	if r.OperationTypes == nil {
		return errUndefined("OperationTypes")
	}
	return nil
}

func errUndefined(field string) error {
	return fmt.Errorf("field %s is not defined", field)
}
