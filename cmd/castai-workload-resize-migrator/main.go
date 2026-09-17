package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"
	"castai-workload-resize-migrator/pkg/migrator"
	"castai-workload-resize-migrator/pkg/workqueue"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	// leaseDuration is how long a non-leader waits before attempting to
	// acquire leadership. Matches the k8s client-go default.
	leaseDuration = 15 * time.Second
	// renewDeadline is how long the leader retries refreshing leadership
	// before giving up. Matches the k8s client-go default.
	renewDeadline = 10 * time.Second
	// retryPeriod is the time between leader election action attempts.
	// Matches the k8s client-go default.
	retryPeriod = 2 * time.Second

	// healthPort is where /healthz and /readyz are served.
	healthPort = ":8081"
)

func main() {
	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	flag.Parse()

	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restCfg, err := buildConfig(kubeconfig)
	if err != nil {
		slog.Error("failed to build kubeconfig", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		slog.Error("failed to create clientset", "error", err)
		os.Exit(1)
	}

	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		slog.Error("failed to create dynamic client", "error", err)
		os.Exit(1)
	}

	if !shouldUseLeaderElection(cfg) {
		if err := runController(ctx, cfg, clientset, dynamicClient); err != nil && err != context.Canceled {
			slog.Error("controller exited with error", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := validateLeaderElectionConfig(cfg); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	identity := cfg.PodName
	if identity == "" {
		identity = string(uuid.NewUUID())
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.PodNamespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	leCfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				slog.Info("acquired leader lease; starting controller",
					"lease", cfg.LeaseName,
					"namespace", cfg.PodNamespace,
					"identity", identity,
				)
				if err := runController(runCtx, cfg, clientset, dynamicClient); err != nil && err != context.Canceled {
					slog.Error("controller exited with error", "error", err)
				}
			},
			OnStoppedLeading: func() {
				slog.Info("lost leader lease; controller stopping",
					"lease", cfg.LeaseName,
					"namespace", cfg.PodNamespace,
					"identity", identity,
				)
			},
		},
		Name: cfg.LeaseName,
	}

	leaderelection.RunOrDie(ctx, leCfg)
}

// runController wires the detector, migrator, workqueue, informers, and
// background loops together and blocks until ctx is cancelled. The leader
// election wrapper (when enabled) calls this from OnStartedLeading with a
// per-leadership context; otherwise main calls it directly.
//
// Returning ctx.Err() lets callers distinguish a clean shutdown from an
// unexpected failure (RunOrDie does not return on the leader path, so the
// returned error is mainly useful for the leader-election-disabled path).
func runController(ctx context.Context, cfg config.Config, clientset kubernetes.Interface, dynamicClient dynamic.Interface) error {
	det := detector.New(clientset, cfg)
	mig := migrator.New(cfg, dynamicClient)

	// Health endpoints. readyCacheSynced flips once informer caches have
	// synced; heartbeat is touched by every controller activity (pod/node
	// events, scans, cleanups, migrations). /healthz fails when the
	// controller has been silent for longer than healthStaleAfter, so a
	// wedged-but-running process gets restarted — safe, because H1
	// adoption rebuilds in-flight tracking on restart.
	var readyCacheSynced atomic.Bool
	var heartbeat atomic.Int64
	touch := func() { heartbeat.Store(time.Now().UnixNano()) }
	touch()
	healthStaleAfter := 3 * cfg.SafetyScanInterval
	if healthStaleAfter < 6*time.Minute {
		healthStaleAfter = 6 * time.Minute
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readyCacheSynced.Load() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("informer caches not synced"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		last := time.Unix(0, heartbeat.Load())
		if time.Since(last) < healthStaleAfter {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "no controller activity for %s", time.Since(last).Round(time.Second))
	})
	healthServer := &http.Server{Addr: healthPort, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		healthServer.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server failed", "error", err)
		}
	}()

	// Route detector callbacks through a typed workqueue so a slow
	// migration create cannot throttle the informer event handler.
	// The queue is drained by a single worker below.
	q := workqueue.New(workqueue.DefaultBufferSize)

	det.SetMigrateFunc(func(_ context.Context, p *detector.PodPendingInfo) error {
		q.Add(p)
		return nil
	})

	// Skip pods the migrator is already handling at the detector, so a
	// stream of informer updates for the same pending pod does not
	// re-populate the queue and re-log. Combined with cluster-state
	// adoption below, this stays accurate across leader failovers.
	det.SetMigrationStateChecker(mig)

	q.Start(ctx, 1, func(ctx context.Context, p *detector.PodPendingInfo) error {
		return mig.Trigger(ctx, []*detector.PodPendingInfo{p})
	})

	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	podInformer := factory.Core().V1().Pods().Informer()
	nodeInformer := factory.Core().V1().Nodes().Informer()

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { touch(); det.OnPodChange(podFrom(obj)) },
		UpdateFunc: func(_, newObj interface{}) { touch(); det.OnPodChange(podFrom(newObj)) },
		DeleteFunc: func(obj interface{}) { touch(); det.OnPodDelete(podFrom(obj)) },
	})

	// Node informer feeds the detector's node-template map, used to scope
	// the controller to pods on specific CAST AI node templates
	// (SOURCE_NODE_TEMPLATES).
	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { det.OnNodeChange(nodeFrom(obj)) },
		UpdateFunc: func(_, newObj interface{}) { det.OnNodeChange(nodeFrom(newObj)) },
		DeleteFunc: func(obj interface{}) { det.OnNodeDelete(nodeFrom(obj)) },
	})

	factory.Start(ctx.Done())
	for t, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			slog.Error("failed to sync informer cache", "type", t)
			return context.Canceled
		}
	}
	readyCacheSynced.Store(true)
	slog.Info("informer caches synced")

	// Re-adopt migrations created by previous controller instances (e.g.
	// before a restart or leader failover) so pods whose migrations are
	// still in flight are not re-triggered with duplicate CRDs. A failure
	// to adopt degrades to in-memory-only dedup rather than crashing the
	// controller on a transient API error at startup.
	if n, err := mig.AdoptExisting(ctx); err != nil {
		slog.Error("failed to adopt existing migrations; continuing without adoption", "error", err)
	} else if n > 0 {
		slog.Info("adopted in-flight migrations from previous instance", "count", n)
	}

	var wg sync.WaitGroup

	// Fallback safety scan — runs periodically to catch any pods missed
	// during informer events (e.g., controller restart, informer cache gap).
	// This is NOT the primary trigger — migrations are created event-driven
	// in OnPodChange. Default interval: 2 minutes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.SafetyScanInterval)
		defer ticker.Stop()
		// Run an initial scan shortly after startup to catch pods that
		// were already pending before the controller started. The delay
		// is context-aware so shutdown is never held hostage by a bare
		// sleep.
		select {
		case <-ctx.Done():
			return
		case <-time.After(cfg.SafetyScanStartupDelay):
		}
		scanSafety(ctx, det, mig)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				touch()
				scanSafety(ctx, det, mig)
			}
		}
	}()

	// Migration cleanup loop — periodically checks and removes completed/failed migrations.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.MigrationCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				touch()
				mig.CleanupCompletedMigrations(ctx)
			}
		}
	}()

	slog.Info("controller started",
		"dryRun", cfg.DryRun,
		"safetyScanInterval", cfg.SafetyScanInterval,
		"safetyScanStartupDelay", cfg.SafetyScanStartupDelay,
		"migrationCleanupInterval", cfg.MigrationCleanupInterval,
		"migrationRateLimitPerHour", cfg.MigrationRateLimitPerHour,
		"maxConcurrentMigrations", cfg.MaxConcurrentMigrations,
		"failedDestinationTTL", cfg.FailedDestinationTTL.String(),
	)

	// Block until shutdown is signalled, and only then drain the queue
	// so in-flight mig.Trigger calls finish under a bounded timeout
	// before waiting on the safety-scan/cleanup loops. Stopping the
	// queue any earlier would silently disable the event-driven path:
	// detector callbacks enqueue into a closed queue and are dropped,
	// leaving only the periodic safety scan to trigger migrations.
	// Stop is safe to call multiple times; workqueue uses sync.Once
	// internally.
	<-ctx.Done()
	q.Stop()
	wg.Wait()
	return ctx.Err()
}

// podFrom extracts a *corev1.Pod from an informer event object. Informer
// handlers normally receive *corev1.Pod directly, but DeleteFunc can
// deliver a tombstone (cache.DeletedFinalStateUnknown) wrapping the last
// known object when a delete happens after an informer resync gap.
// Asserting directly on the tombstone would panic and crash the
// controller, so unwrap it first; anything that is still not a Pod
// yields nil, which the detector handlers treat as a no-op.
func podFrom(obj interface{}) *corev1.Pod {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod
	}
	return nil
}

// nodeFrom unwraps informer objects into *corev1.Node, tolerating the
// DeletedFinalStateUnknown tombstone that DeleteFunc may deliver after
// an informer resync gap. Mirrors podFrom.
func nodeFrom(obj interface{}) *corev1.Node {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	if node, ok := obj.(*corev1.Node); ok {
		return node
	}
	return nil
}

// shouldUseLeaderElection reports whether the controller should run the
// leader-election startup path. It gates purely on the LeaderElection
// flag; the missing-namespace check that fatal-s main() lives in
// validateLeaderElectionConfig and is intentionally NOT this helper's
// responsibility.
//
// Extracted so the gating decision can be unit-tested without spinning
// up an informer factory or the leaderelection package.
func shouldUseLeaderElection(cfg config.Config) bool {
	return cfg.LeaderElection
}

// validateLeaderElectionConfig ensures the configuration is consistent
// for the leader-election-enabled startup path. Today the only check is
// that POD_NAMESPACE is non-empty: a LeaseLock requires a namespace, and
// silently falling back to the direct path on a misconfigured deploy
// would hide the bug. Returning an error lets main() log and os.Exit(1)
// in one place; the disabled path never reaches this helper, so the
// permissive "POD_NAMESPACE may be empty when election is off"
// invariant is preserved.
func validateLeaderElectionConfig(cfg config.Config) error {
	if cfg.PodNamespace == "" {
		return fmt.Errorf("leader election enabled but POD_NAMESPACE is empty; set it via downward API")
	}
	return nil
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func scanSafety(ctx context.Context, det *detector.Detector, mig *migrator.Client) {
	slog.Info("running fallback safety scan")
	pods := det.ListSuspectPods(ctx)
	if len(pods) == 0 {
		slog.Debug("safety scan found no suspect pods")
		return
	}
	if err := mig.Trigger(ctx, pods); err != nil {
		slog.Error("safety scan migration trigger failed", "error", err)
	}
}

// Reference the coordination v1 client package indirectly via the
// clientset.CoordinationV1() call above; this comment documents that the
// import is intentional and consumed transitively by the LeaseLock Client
// field assignment.
var _ coordinationv1.CoordinationV1Interface = (*coordinationv1.CoordinationV1Client)(nil)
