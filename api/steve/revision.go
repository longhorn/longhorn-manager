package steve

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// listRevisionRewriter wraps the Steve handler that serves Rancher aggregation
// requests so that collection (list) responses advertise a resourceVersion the
// downstream cluster-agent watch cache can actually resume from.
//
// Background: Rancher routes HTTP list/get for longhorn.io.* types through the
// aggregation tunnel to longhorn-manager's Steve handler, but the dashboard's
// websocket watch (/v1/subscribe) is served by the cluster-agent's local Steve
// cache. The cluster-agent cache keys its event log by each object's Kubernetes
// resourceVersion, whereas the proxy store backing longhorn-manager returns the
// global etcd resourceVersion as the collection revision. That global revision
// never exists in the cluster-agent event log, so the dashboard's watch fails
// with a permanent "resourceversion too old" error and the list shows up empty.
//
// Rewriting the collection revision to the maximum object resourceVersion in the
// list keeps list and watch in the same revision space, fixing the empty-list
// problem without changing Rancher.
func listRevisionRewriter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			next.ServeHTTP(rw, req)
			return
		}

		rec := &revisionRecorder{ResponseWriter: rw, status: http.StatusOK}
		next.ServeHTTP(rec, req)
		rec.flush()
	})
}

// revisionRecorder buffers a handler's response so the collection revision can
// be rewritten before it is sent upstream through the aggregation tunnel.
type revisionRecorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *revisionRecorder) WriteHeader(code int) {
	r.status = code
}

func (r *revisionRecorder) Write(b []byte) (int, error) {
	return r.buf.Write(b)
}

func (r *revisionRecorder) flush() {
	body := r.buf.Bytes()
	ct := r.Header().Get("Content-Type")
	gzipped := strings.Contains(strings.ToLower(r.Header().Get("Content-Encoding")), "gzip")

	if r.status == http.StatusOK && strings.Contains(ct, "application/json") {
		decoded := body
		if gzipped {
			if plain, err := gunzip(body); err == nil {
				decoded = plain
			} else {
				logrus.WithError(err).Warn("Steve aggregation: failed to gunzip list response; leaving it unchanged")
				decoded = nil
			}
		}

		if decoded != nil {
			if rewritten, ok := rewriteCollectionRevision(decoded); ok {
				// Emit the rewritten body uncompressed and drop the gzip encoding
				// so downstream consumers read the corrected revision directly.
				body = rewritten
				r.Header().Del("Content-Encoding")
			}
		}
	}

	r.Header().Set("Content-Length", strconv.Itoa(len(body)))
	r.ResponseWriter.WriteHeader(r.status)
	_, _ = r.ResponseWriter.Write(body)
}

// rewriteCollectionRevision replaces a Steve collection's revision with the
// maximum object resourceVersion found in its data. It returns the rewritten
// body and true only when the response is a collection; otherwise it returns
// false and the original body is left untouched.
func rewriteCollectionRevision(body []byte) ([]byte, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, false
	}

	typeRaw, ok := m["type"]
	if !ok {
		return nil, false
	}
	var typeStr string
	if err := json.Unmarshal(typeRaw, &typeStr); err != nil || typeStr != "collection" {
		return nil, false
	}

	dataRaw, ok := m["data"]
	if !ok {
		return nil, false
	}

	var items []struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(dataRaw, &items); err != nil {
		return nil, false
	}

	var maxRV uint64
	found := false
	for _, it := range items {
		if it.Metadata.ResourceVersion == "" {
			continue
		}
		rv, err := strconv.ParseUint(it.Metadata.ResourceVersion, 10, 64)
		if err != nil {
			continue
		}
		if rv > maxRV {
			maxRV = rv
			found = true
		}
	}

	if !found {
		// Empty list (or non-numeric resourceVersions): drop the collection
		// revision so the dashboard watch starts from the current state instead
		// of a global revision the cluster-agent cache does not know about.
		delete(m, "revision")
	} else {
		rev, err := json.Marshal(strconv.FormatUint(maxRV, 10))
		if err != nil {
			return nil, false
		}
		m["revision"] = rev
	}

	out, err := json.Marshal(m)
	if err != nil {
		return nil, false
	}

	logrus.Tracef("Steve aggregation: rewrote collection revision to max object resourceVersion %d", maxRV)
	return out, true
}

// gunzip decompresses a gzip-encoded body.
func gunzip(body []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = gr.Close() }()
	return io.ReadAll(gr)
}
