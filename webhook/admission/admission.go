package admission

import (
	"fmt"
	"strings"

	"github.com/rancher/wrangler/pkg/webhook"
	"github.com/sirupsen/logrus"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"

	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

const (
	AdmissionTypeValidation = "validaton"
	AdmissionTypeMutation   = "mutation"
)

// PatchOps JSON Patch operations to mutate input data. See https://jsonpatch.com/ for more information.
type PatchOps []string

// A Admitter interface is used by AdmissionHandler to check if a operation is allowed.
type Admitter interface {
	// Create checks if a CREATE operation is allowed.
	// PatchOps contains JSON patch operations to be applied on the API object received by the server.
	// If no error is returned, the operation is allowed.
	Create(request *Request, newObj runtime.Object) (PatchOps, error)

	// Update checks if a UPDATE operation is allowed.
	// PatchOps contains JSON patch operations to be applied on the API object received by the server.
	// If no error is returned, the operation is allowed.
	Update(request *Request, oldObj runtime.Object, newObj runtime.Object) (PatchOps, error)

	// Delete checks if a DELETE operation is allowed.
	// PatchOps contains JSON patch operations to be applied on the API object received by the server.
	// If no error is returned, the operation is allowed.
	Delete(request *Request, oldObj runtime.Object) (PatchOps, error)

	// Connect checks if a CONNECT operation is allowed.
	// PatchOps contains JSON patch operations to be applied on the API object received by the server.
	// If no error is returned, the operation is allowed.
	Connect(request *Request, newObj runtime.Object) (PatchOps, error)

	// Resource returns the resource that the admitter works on.
	Resource() Resource
}

type Handler struct {
	admitter      Admitter
	admissionType string
	logger        logrus.FieldLogger
}

func NewHandler(admitter Admitter, admissionType string) *Handler {
	if err := admitter.Resource().Validate(); err != nil {
		panic(err.Error())
	}
	return &Handler{
		admitter:      admitter,
		admissionType: admissionType,
		logger:        logrus.StandardLogger().WithField("service", "admissionWebhook"),
	}
}

func (v *Handler) Admit(response *webhook.Response, request *webhook.Request) error {
	v.admit(response, NewRequest(request))
	return nil
}

func (v *Handler) admit(response *webhook.Response, req *Request) {
	oldObj, newObj, err := req.DecodeObjects()
	if err != nil {
		v.logger.Errorf("%s failed to decode objects: %s", req, err)
		response.Result = werror.NewInternalError(err.Error()).AsResult()
		response.Allowed = false
		return
	}

	var patchOps PatchOps

	switch req.Operation {
	case admissionv1.Create:
		patchOps, err = v.admitter.Create(req, newObj)
	case admissionv1.Delete:
		patchOps, err = v.admitter.Delete(req, oldObj)
	case admissionv1.Update:
		patchOps, err = v.admitter.Update(req, oldObj, newObj)
	case admissionv1.Connect:
		patchOps, err = v.admitter.Connect(req, newObj)
	default:
		err = fmt.Errorf("unsupported operation %s", req.Operation)
	}

	if err != nil {
		var admitErr werror.AdmitError
		if e, ok := err.(werror.AdmitError); ok {
			admitErr = e
		} else {
			admitErr = werror.NewInternalError(err.Error())
		}
		response.Allowed = false
		response.Result = admitErr.AsResult()
		v.logger.WithError(admitErr).Warnf("Rejected operation: %s", req)
		return
	}

	if len(patchOps) > 0 {
		patchType := admissionv1.PatchTypeJSONPatch
		patchData := fmt.Sprintf("[%s]", strings.Join(patchOps, ","))
		response.PatchType = &patchType
		response.Patch = []byte(patchData)
		v.logger.Infof("%s patchOps: %s", req, patchData)
	}

	response.Allowed = true
}

func (v *Handler) decodeObjects(request *Request) (oldObj runtime.Object, newObj runtime.Object, err error) {
	operation := request.Operation
	if operation == admissionv1.Delete || operation == admissionv1.Update {
		oldObj, err = request.DecodeOldObject()
		if err != nil {
			return
		}
		if operation == admissionv1.Delete {
			// no new object for DELETE operation
			return
		}
	}
	newObj, err = request.DecodeObject()
	return
}
