package admission

import (
	"fmt"

	"github.com/rancher/wrangler/pkg/webhook"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Request struct {
	*webhook.Request
}

func NewRequest(webhookRequest *webhook.Request) *Request {
	return &Request{
		Request: webhookRequest,
	}
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
