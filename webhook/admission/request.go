package admission

import (
	"fmt"

	"github.com/rancher/wrangler/v3/pkg/webhook"

	"k8s.io/apimachinery/pkg/runtime"

	admissionv1 "k8s.io/api/admission/v1"
)

type Request struct {
	*webhook.Request

	warnings []string
}

func NewRequest(webhookRequest *webhook.Request) *Request {
	return &Request{
		Request: webhookRequest,
	}
}

// AddWarning records a warning to return with the admission response.
// It does not affect whether the operation is allowed; kubectl prints the
// warning either way. This lets an admitter allow a request while still
// pointing out a condition the user should know about.
func (r *Request) AddWarning(warning string) {
	r.warnings = append(r.warnings, warning)
}

// Warnings returns the warnings recorded so far, in order.
func (r *Request) Warnings() []string {
	return r.warnings
}

func (r *Request) Username() string {
	return r.UserInfo.Username
}

func (r *Request) IsGarbageCollection() bool {
	return r.Operation == admissionv1.Delete
}

func (r *Request) DecodeObjects() (oldObj runtime.Object, newObj runtime.Object, err error) {
	operation := r.Operation
	if operation == admissionv1.Delete || operation == admissionv1.Update {
		oldObj, err = r.DecodeOldObject()
		if err != nil {
			return
		}
		if operation == admissionv1.Delete {
			// no new object for DELETE operation
			return
		}
	}
	newObj, err = r.DecodeObject()
	return
}

func (r *Request) String() string {
	return fmt.Sprintf("Request (user: %s, %s, namespace: %s, name: %s, operation: %s)", r.UserInfo.Username, r.Kind, r.Namespace, r.Name, r.Operation)
}
