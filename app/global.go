package app

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"
)

const (
	// HealthzPortGlobalManager serves /v1/healthz, which returns 200 OK
	// whenever the process is alive; it is not tied to leader status so
	// standby pods stay Ready.
	HealthzPortGlobalManager = ":9505"
)

// GlobalCmd returns the `longhorn-manager global` subcommand, which hosts the
// controllers that need cluster-wide Pod visibility (KubernetesPVController,
// KubernetesPodController) under leader election.
func GlobalCmd() *cli.Command {
	return &cli.Command{
		Name:  "global",
		Usage: "Run the longhorn-global-manager workload (leader-elected).",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if err := startGlobalManager(cmd); err != nil {
				logrus.Fatalf("Error starting global manager: %v", err)
			}
			return nil
		},
	}
}

func startGlobalManager(cmd *cli.Command) error {
	kubeconfigPath := cmd.String(FlagKubeConfig)

	podName, err := util.GetRequiredEnv(types.EnvPodName)
	if err != nil {
		return errors.Wrap(err, "failed to detect the pod name")
	}
	podNamespace, err := util.GetRequiredEnv(types.EnvPodNamespace)
	if err != nil {
		return errors.Wrap(err, "failed to detect the pod namespace")
	}

	ctx := signals.SetupSignalContext()

	logger := logrus.StandardLogger().WithFields(logrus.Fields{
		"component": "longhorn-global-manager",
		"pod":       podName,
	})

	// A controller-init or healthz-listener failure must not exit in place:
	// recording the error and cancelling the election context lets RunOrDie
	// unwind and release the Lease (ReleaseOnCancel) before the process exits
	// through the normal return path. The mutex makes the error safe to read
	// after RunOrDie returns — client-go does not wait for the
	// OnStartedLeading goroutine.
	electionCtx, cancelElection := context.WithCancel(ctx)
	defer cancelElection()

	var (
		failureLock sync.Mutex
		failure     error
	)
	fail := func(err error) {
		failureLock.Lock()
		if failure == nil {
			failure = err
		}
		failureLock.Unlock()
		cancelElection()
	}

	startHealthzServer(ctx, logger, func(err error) {
		fail(errors.Wrap(err, "healthz server failed"))
	})

	// Build clients/informers before leader election so every replica keeps a
	// warm cache; a new leader starts against a synced cache (no fresh LIST).
	clients, err := client.NewClients(kubeconfigPath, true, electionCtx.Done())
	if err != nil {
		return errors.Wrap(err, "failed to build longhorn clients")
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      types.LonghornGlobalManagerName,
			Namespace: podNamespace,
		},
		Client: clients.K8s.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	leaderelection.RunOrDie(electionCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   20 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				logger.Info("Acquired leader lease; starting global controllers")
				if err := controller.StartGlobalControllers(logger, clients, leaderCtx.Done()); err != nil {
					fail(errors.Wrap(err, "failed to start global controllers"))
					return
				}
				logger.Info("Global controllers started; serving as leader")
				<-leaderCtx.Done()
			},
			OnStoppedLeading: func() {
				// Runs on both lease loss and election cancel; RunOrDie
				// returns after this, so the process exits via the normal
				// return path.
				logger.Info("Leader election stopped; shutting down")
			},
			OnNewLeader: func(identity string) {
				if identity == podName {
					return
				}
				logger.Infof("Standing by; current leader is %q", identity)
			},
		},
	})

	failureLock.Lock()
	defer failureLock.Unlock()
	return failure
}

// startHealthzServer backs the Deployment's liveness probe. The server shuts
// down when ctx is cancelled so SIGTERM stops the listener promptly, and a
// listener failure is reported through onFailure so the caller can release
// leadership before exiting.
func startHealthzServer(ctx context.Context, logger logrus.FieldLogger, onFailure func(error)) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{
		Addr:              HealthzPortGlobalManager,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Infof("Healthz server listening on %s", HealthzPortGlobalManager)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.WithError(err).Error("Healthz server failed")
			onFailure(err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.WithError(err).Warn("Healthz server shutdown error")
		}
	}()
}
