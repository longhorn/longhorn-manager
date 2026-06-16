package steve

import (
	"context"
	"net/http"
	"os"

	"github.com/rancher/steve/pkg/aggregation"
	"github.com/sirupsen/logrus"

	corectl "github.com/rancher/wrangler/v3/pkg/generated/controllers/core"

	"k8s.io/client-go/rest"
)

// DefaultAggregationSecretName is the Secret name longhorn-manager watches
// for Steve aggregation credentials. When longhorn-manager creates a
// management.cattle.io/v3 APIService CR (see apiservice.go), Rancher reacts
// by populating this Secret with the tunnel URL/token/CA. longhorn-manager
// then opens a remotedialer tunnel back to Rancher so that requests matching
// the APIService PathPrefixes are forwarded to the local Steve handler.
const (
	DefaultAggregationSecretName = "longhorn-aggregation"
)

// AggregationSecretName returns the Secret name to watch, allowing operators
// to override it with LONGHORN_STEVE_AGGREGATION_SECRET.
func AggregationSecretName() string {
	if v := os.Getenv("LONGHORN_STEVE_AGGREGATION_SECRET"); v != "" {
		return v
	}
	return DefaultAggregationSecretName
}

// StartAggregation registers a Secret watcher that, when populated by Rancher,
// connects longhorn-manager's Steve handler to Rancher's Steve aggregation
// tunnel. It is a no-op until the Secret exists and contains url/token.
//
// namespace is the namespace where the aggregation Secret lives (typically the
// pod namespace, e.g. longhorn-system). handler is the Steve handler that will
// serve aggregated requests; pass the full Steve handler so request paths like
// /v1/longhorn.io.volume/... are accepted as-is.
func StartAggregation(ctx context.Context, restConfig *rest.Config, namespace, secretName string, handler http.Handler) error {
	if namespace == "" || secretName == "" || handler == nil {
		logrus.Warn("Steve aggregation not started: namespace, secretName and handler are required")
		return nil
	}

	factory, err := corectl.NewFactoryFromConfigWithOptions(restConfig, &corectl.FactoryOptions{
		Namespace: namespace,
	})
	if err != nil {
		return err
	}

	aggregation.Watch(ctx, factory.Core().V1().Secret(), namespace, secretName, handler)

	if err := factory.Start(ctx, 1); err != nil {
		return err
	}

	logrus.Infof("Steve aggregation watcher started: namespace=%q secret=%q", namespace, secretName)
	return nil
}
