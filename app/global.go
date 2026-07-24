package app

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/controller"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"
)

const (
	// GlobalControllerID is the controller identity for the controllers hosted
	// in the longhorn-global-manager Deployment.
	GlobalControllerID = "longhorn-global-manager"

	// LeaseLockNameGlobalManager is the Lease used by the global-manager
	// Deployment for leader election.
	LeaseLockNameGlobalManager = "longhorn-global-manager"

	// HealthzPortGlobalManager — /v1/healthz returns 200 OK whenever
	// the process is alive (not tied to leader status, so standby pods
	// stay Ready).
	HealthzPortGlobalManager = ":9505"
)

// GlobalCmd — `longhorn-manager global` subcommand. Hosts the
// controllers that need cluster-wide Pod visibility (KubernetesPV/
// PodController) under leader election. See
// enhancements/20260506-global-longhorn-manager.md.
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
		return fmt.Errorf("failed to detect the pod name (env %v)", types.EnvPodName)
	}
	podNamespace, err := util.GetRequiredEnv(types.EnvPodNamespace)
	if err != nil {
		return fmt.Errorf("failed to detect the pod namespace (env %v)", types.EnvPodNamespace)
	}

	ctx := signals.SetupSignalContext()

	logger := logrus.StandardLogger().WithFields(logrus.Fields{
		"component": "longhorn-global-manager",
		"pod":       podName,
	})

	startHealthzServer(ctx, logger)

	// Build clients/informers before leader election so every replica keeps a
	// warm cache; a new leader starts against a synced cache (no fresh LIST).
	clients, err := client.NewClients(kubeconfigPath, true, ctx.Done())
	if err != nil {
		return errors.Wrap(err, "failed to build longhorn clients")
	}

	config, err := buildLeaseConfig(kubeconfigPath)
	if err != nil {
		return errors.Wrap(err, "failed to build kube config for lease")
	}
	leaseKubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to build kube client for lease")
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      LeaseLockNameGlobalManager,
			Namespace: podNamespace,
		},
		Client: leaseKubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	// Controller-init failure is propagated by cancelling the election
	// context instead of exiting inside the callback, so RunOrDie unwinds
	// and releases the Lease (ReleaseOnCancel) before the process exits.
	var controllerInitErr error
	electionCtx, cancelElection := context.WithCancel(ctx)
	defer cancelElection()

	leaderelection.RunOrDie(electionCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   20 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				logger.Info("Acquired leader lease; starting global controllers")
				if err := controller.StartGlobalControllers(logger, clients, GlobalControllerID); err != nil {
					controllerInitErr = err
					cancelElection()
					return
				}
				logger.Info("Global controllers started; serving as leader")
				<-leaderCtx.Done()
				logger.Info("Leader context cancelled")
			},
			OnStoppedLeading: func() {
				// Runs on lease loss and on election-context cancel. No
				// os.Exit here — RunOrDie returns right after this and the
				// process exits through the normal return path, so a demoted
				// leader stops writing and the Deployment recreates the pod.
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

	if controllerInitErr != nil {
		return errors.Wrap(controllerInitErr, "failed to start global controllers")
	}
	return nil
}

func buildLeaseConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
}

// startHealthzServer — backs the Deployment's liveness probe.
// Shuts down when ctx is cancelled so SIGTERM stops the listener
// promptly instead of waiting for process termination.
func startHealthzServer(ctx context.Context, logger logrus.FieldLogger) {
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
			logger.WithError(err).Fatal("Healthz server failed")
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
