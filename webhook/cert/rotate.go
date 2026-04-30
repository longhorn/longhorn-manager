package cert

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Separate from LeaseLockNameWebhook: that lease gates a one-shot
	// webhook init with an aggressive timeout; rotation is a long-running
	// periodic worker that wants a calmer lease.
	rotationLeaseName = "longhorn-webhook-cert-rotation"

	// Matches dynamiclistener's 6h check cadence.
	rotationCheckInterval = 6 * time.Hour

	// Short backoff so a transient apiserver hiccup doesn't idle us for 6h.
	rotationErrorRetryInterval = 5 * time.Minute
)

// RotateLoop runs leader-elected EnsureWebhookSecrets ticks until ctx is
// cancelled. Single-writer is the property we care about; non-leaders pick up
// rotated material through the existing dynamiclistener watch path with no
// pod restart. Call once per pod with a unique identity (node or pod name).
func RotateLoop(ctx context.Context, k8s kubernetes.Interface, namespace, identity string) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      rotationLeaseName,
			Namespace: namespace,
		},
		Client: k8s.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second,
		RenewDeadline:   30 * time.Second,
		RetryPeriod:     10 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				logrus.Infof("Webhook cert rotation leader elected: %s", identity)
				runRotationWorker(leaderCtx, k8s, namespace)
			},
			OnStoppedLeading: func() {
				logrus.Infof("Webhook cert rotation leader lost: %s", identity)
			},
			OnNewLeader: func(id string) {
				if id != identity {
					logrus.Infof("Webhook cert rotation leader changed: %s", id)
				}
			},
		},
	})
}

func runRotationWorker(ctx context.Context, k8s kubernetes.Interface, namespace string) {
	// Sync immediately on becoming leader so a near-expiry cert doesn't wait
	// a full rotationCheckInterval after the previous leader died.
	wait := runOnce(ctx, k8s, namespace, true)
	// Reuse a single Timer so an in-flight tick is stopped on ctx cancel
	// instead of leaking until it fires.
	t := time.NewTimer(wait)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			wait = runOnce(ctx, k8s, namespace, false)
			t.Reset(wait)
		}
	}
}

func runOnce(ctx context.Context, k8s kubernetes.Interface, namespace string, initial bool) time.Duration {
	if err := EnsureWebhookSecrets(ctx, k8s, namespace); err != nil {
		phase := "periodic"
		if initial {
			phase = "initial"
		}
		logrus.WithError(err).Errorf("Failed %s webhook cert rotation check", phase)
		return rotationErrorRetryInterval
	}
	return rotationCheckInterval
}
