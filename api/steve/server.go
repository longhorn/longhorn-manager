package steve

import (
	"context"
	"net/http"

	"github.com/rancher/steve/pkg/auth"
	"github.com/sirupsen/logrus"

	steveserver "github.com/rancher/steve/pkg/server"

	"k8s.io/client-go/rest"
)

// Server wraps a Steve server that provides Steve-style API endpoints
// for Longhorn CRDs (e.g., /v1/longhorn.io.volumes)
type Server struct {
	*steveserver.Server
}

// Options configures the Steve server
type Options struct {
	// AuthMiddleware for authentication, if nil uses AlwaysAdmin
	AuthMiddleware auth.Middleware
	// Next handler for routes not handled by Steve
	Next http.Handler
}

// New creates a new Steve server that automatically discovers and serves
// all Kubernetes CRDs including Longhorn resources in Steve API format.
//
// The Steve server will automatically:
// - Connect to the Kubernetes API server
// - Discover all CRDs (including longhorn.io resources)
// - Serve them at /v1/{group}.{resource} format
// - Handle spec/status serialization automatically
func New(ctx context.Context, restConfig *rest.Config, opts *Options) (*Server, error) {
	if opts == nil {
		opts = &Options{}
	}

	steveOpts := &steveserver.Options{}

	if opts.AuthMiddleware != nil {
		steveOpts.AuthMiddleware = opts.AuthMiddleware
	}

	if opts.Next != nil {
		steveOpts.Next = opts.Next
	}

	steve, err := steveserver.New(ctx, restConfig, steveOpts)
	if err != nil {
		return nil, err
	}

	// Register Longhorn schema templates for custom actions
	// This must be done after server creation but before it starts serving
	registerSchemaTemplates(steve)

	logrus.Info("Steve server initialized - Longhorn CRDs will be available at /apis/v1/longhorn.io.*")

	return &Server{Server: steve}, nil
}

// AggregationHandler returns the Steve handler used to serve requests forwarded
// through Rancher's aggregation tunnel. It wraps the full Steve handler with
// listRevisionRewriter so that collection responses carry a resourceVersion the
// downstream cluster-agent watch cache can resume from, preventing the
// "resourceversion too old" error that otherwise leaves the UI list empty.
func (s *Server) AggregationHandler() http.Handler {
	return listRevisionRewriter(s.Server)
}

// SimplifiedHandler returns the HTTP handler with simplified URL support.
// It rewrites paths like /v1/volumes/{name} to /v1/longhorn.io.volumes/longhorn-system/{name}
// and rewrites response URLs back to the simplified format.
func (s *Server) SimplifiedHandler(namespace string) http.Handler {
	return SimplifiedPathMiddleware(namespace, s.Server)
}

// SetLegacyHandler registers Longhorn's legacy HTTP router so that Steve
// action handlers (POST ?action=*) can forward into the existing
// /v1/{resource}/{name}?action=... endpoints that hold the real action logic.
// Must be called after the legacy router has been constructed.
func (s *Server) SetLegacyHandler(h http.Handler) {
	SetLegacyHandler(h)
}
