package app

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rancher/wrangler/v3/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/hookexecutor/podexec"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook/runner"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

const (
	// HealthzPortHookExecutor serves /v1/healthz and /metrics for the
	// executor Deployment.
	HealthzPortHookExecutor = ":9506"

	// FlagSnapshotHookPauseWorkers bounds how many SnapshotGroups run
	// pre-commands at once. Releases run in a separate pool at least as
	// large, so resuming applications never waits behind pause work.
	FlagSnapshotHookPauseWorkers = "pause-workers"

	hookExecutorResyncPeriod = 30 * time.Second
)

// HookExecutorCmd returns the `longhorn-manager hook-executor` subcommand.
// The leader-elected process runs VolumeHookPolicy pre and post commands
// inside application pods when SnapshotGroups request hooks.
func HookExecutorCmd() *cli.Command {
	return &cli.Command{
		Name:  "hook-executor",
		Usage: "Run the longhorn-hook-executor workload (leader-elected).",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  FlagKubeConfig,
				Usage: "Specify path to kube config (optional)",
			},
			&cli.IntFlag{
				Name:  FlagSnapshotHookPauseWorkers,
				Usage: "Specify how many SnapshotGroups run their pre-commands at once; at least as many run post-commands at once",
				Value: runner.DefaultPauseWorkers,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if err := startHookExecutor(cmd); err != nil {
				logrus.Fatalf("Error starting hook executor: %v", err)
			}
			return nil
		},
	}
}

func startHookExecutor(cmd *cli.Command) error {
	kubeconfigPath := cmd.String(FlagKubeConfig)
	pauseWorkers := cmd.Int(FlagSnapshotHookPauseWorkers)
	if pauseWorkers < 1 {
		return errors.Errorf("--%v must be at least 1, got %v", FlagSnapshotHookPauseWorkers, pauseWorkers)
	}

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
		"component": types.LonghornHookExecutorName,
		"pod":       podName,
	})

	// A failure in the leader callback cancels the election, releasing
	// the Lease before exit.
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

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return errors.Wrap(err, "unable to get client config")
	}
	config.QPS = types.KubeAPIQPS
	config.Burst = types.KubeAPIBurst

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get k8s client")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "unable to get longhorn client")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "unable to create scheme")
	}

	metricsRegistry := prometheus.NewRegistry()
	if err := runner.RegisterMetrics(metricsRegistry); err != nil {
		return errors.Wrap(err, "failed to register executor metrics")
	}
	startExecutorHTTPServer(ctx, logger, metricsRegistry, func(err error) {
		fail(errors.Wrap(err, "healthz server failed"))
	})

	// Informers start before the election so a new leader begins with a
	// synced cache.
	informerFactory := lhinformers.NewSharedInformerFactoryWithOptions(lhClient, hookExecutorResyncPeriod,
		lhinformers.WithNamespace(podNamespace))
	groupInformer := informerFactory.Longhorn().V1beta2().SnapshotGroups()
	volumeInformer := informerFactory.Longhorn().V1beta2().Volumes()
	settingInformer := informerFactory.Longhorn().V1beta2().Settings()
	store := runner.NewKubeStore(podNamespace, lhClient, kubeClient,
		groupInformer.Lister(), volumeInformer.Lister(), settingInformer.Lister())
	// Run syncs the group informer itself; the other two are synced here.
	groupSharedInformer := groupInformer.Informer()
	volumeSynced := volumeInformer.Informer().HasSynced
	settingSynced := settingInformer.Informer().HasSynced
	informerFactory.Start(electionCtx.Done())

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})
	defer eventBroadcaster.Shutdown()
	recorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: types.LonghornHookExecutorName})

	exec := runner.New(logger, store, podexec.NewRemoteCommandExecutor(config, kubeClient), recorder, pauseWorkers)

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      types.LonghornHookExecutorName,
			Namespace: podNamespace,
		},
		Client: kubeClient.CoordinationV1(),
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
				logger.Info("Acquired leader lease; starting hook executor")
				if !waitForSynced(leaderCtx, volumeSynced, settingSynced) {
					return
				}
				if err := exec.Run(leaderCtx, groupSharedInformer); err != nil {
					fail(errors.Wrap(err, "hook executor failed"))
				}
			},
			OnStoppedLeading: func() {
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

// waitForSynced waits until every HasSynced function reports true,
// returning false when ctx ends first.
func waitForSynced(ctx context.Context, synced ...func() bool) bool {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		all := true
		for _, fn := range synced {
			if !fn() {
				all = false
				break
			}
		}
		if all {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// startExecutorHTTPServer serves the liveness probe and metrics on one
// listener until ctx is cancelled. A listener failure is reported through
// onFailure so leadership is released before exiting.
func startExecutorHTTPServer(ctx context.Context, logger logrus.FieldLogger, gatherer prometheus.Gatherer, onFailure func(error)) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	server := &http.Server{
		Addr:              HealthzPortHookExecutor,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Infof("Healthz and metrics server listening on %s", HealthzPortHookExecutor)
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
