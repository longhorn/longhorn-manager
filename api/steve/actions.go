package steve

import (
	"net/http"
	"path"
	"strings"
	"sync/atomic"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// legacyRouterRef holds a reference to Longhorn's legacy HTTP router so that
// Steve action handlers can forward POST ?action=* requests to the existing
// /v1/{resource}/{name}?action=... endpoints that own the real action logic.
//
// It is set by SetLegacyHandler once the router has been built. Reads happen at
// request time, so the order between schema template registration and router
// construction does not matter.
var legacyRouterRef atomic.Value // stores http.Handler

// SetLegacyHandler registers the legacy Longhorn HTTP router for action
// forwarding. Calling with nil is a no-op.
func SetLegacyHandler(h http.Handler) {
	if h == nil {
		return
	}
	legacyRouterRef.Store(handlerHolder{h})
}

// handlerHolder is a concrete type wrapper so atomic.Value sees a stable type.
type handlerHolder struct{ http.Handler }

func loadLegacyHandler() http.Handler {
	if v, ok := legacyRouterRef.Load().(handlerHolder); ok {
		return v.Handler
	}
	return nil
}

// bridgeAction returns an http.Handler that forwards a Steve action POST to
// the legacy Longhorn router at /v1/{legacyResource}/{name}?action={action}.
//
//	legacyResource is the legacy plural path segment (e.g. "volumes",
//	"nodes", "backupvolumes", "backuptargets").
//	legacyVar is the gorilla/mux variable name the legacy handler reads,
//	e.g. "name", "backupVolumeName" or "backupTargetName".
func bridgeAction(legacyResource, legacyVar string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		legacy := loadLegacyHandler()
		if legacy == nil {
			http.Error(w, "longhorn legacy router is not initialized", http.StatusServiceUnavailable)
			return
		}

		name := resourceNameFromRequest(r)
		if name == "" {
			http.Error(w, "could not determine resource name from URL", http.StatusBadRequest)
			return
		}

		action := r.URL.Query().Get("action")
		if action == "" {
			http.Error(w, "missing action query parameter", http.StatusBadRequest)
			return
		}

		newReq := r.Clone(r.Context())
		newReq.URL.Path = "/v1/" + legacyResource + "/" + name
		newReq.URL.RawPath = ""
		newReq.RequestURI = ""
		newReq = mux.SetURLVars(newReq, map[string]string{legacyVar: name})

		logrus.Debugf("steve action bridge: %s %s?%s -> %s?%s",
			r.Method, r.URL.Path, r.URL.RawQuery, newReq.URL.Path, newReq.URL.RawQuery)

		legacy.ServeHTTP(w, newReq)
	})
}

// resourceNameFromRequest extracts the resource name from a Steve action URL.
// Steve dispatches ActionHandlers through rancher/apiserver, not gorilla/mux,
// so mux.Vars is empty. The URL path looks like one of:
//
//	/v1/longhorn.io.volumes/<namespace>/<name>            (namespaced)
//	/v1/longhorn.io.nodes/<namespace>/<name>              (namespaced)
//	/v1/longhorn.io.backupbackingimages/<name>            (cluster-scoped)
//
// For both shapes the resource name is the final path segment.
func resourceNameFromRequest(r *http.Request) string {
	if v := mux.Vars(r)["name"]; v != "" {
		return v
	}
	p := strings.TrimRight(r.URL.Path, "/")
	if p == "" {
		return ""
	}
	return path.Base(p)
}

// bridgeMap builds a map[action]http.Handler where every action forwards to
// /v1/{legacyResource}/{name}?action={action} with mux var {legacyVar} set on
// the forwarded request.
func bridgeMap(legacyResource, legacyVar string, actions ...string) map[string]http.Handler {
	out := make(map[string]http.Handler, len(actions))
	for _, a := range actions {
		out[a] = bridgeAction(legacyResource, legacyVar)
	}
	return out
}
