package server

import (
	"net/http"
	"reflect"

	"github.com/rancher/wrangler/pkg/webhook"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/webhook/admission"
)

func addHandler(router *webhook.Router, admissionType string, admitter admission.Admitter) {
	rsc := admitter.Resource()
	kind := reflect.Indirect(reflect.ValueOf(rsc.ObjectType)).Type().Name()
	router.Kind(kind).Group(rsc.APIGroup).Type(rsc.ObjectType).Handle(admission.NewHandler(admitter, admissionType))
	logrus.Infof("Add %s handler for %s.%s (%s)", admissionType, rsc.Name, rsc.APIGroup, kind)
}

type healthzHandler struct {
}

func newhealthzHandler() *healthzHandler {
	return &healthzHandler{}
}

func (h *healthzHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}
