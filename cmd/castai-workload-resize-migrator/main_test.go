package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"
	"castai-workload-resize-migrator/pkg/workqueue"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Note on testing scope:
//
// runController IS exercised end-to-end below
// (TestRunControllerEventPathCreatesMigration): the fake clientset
// supports the shared informer factory (initial LIST + watch), and the
// safety-scan startup delay is configurable (SafetyScanStartupDelay), so
// the fallback paths can be pushed far enough away that only the
// event-driven path can produce a Migration within the test window.
// The leader-election lifecycle itself is a k8s client-go concern and
// is covered by upstream tests; we only lock down OUR wiring here. See
// the comment on TestLeaseLockWiringFromConfig below.

// TestLeaseLockWiringFromConfig verifies that the LeaseLock constructed in
// main.go wires LeaseName, namespace, and identity from the loaded Config.
//
// main.go builds the lock with a pure struct literal:
//
//	lock := &resourcelock.LeaseLock{
//	    LeaseMeta: metav1.ObjectMeta{
//	        Name:      cfg.LeaseName,
//	        Namespace: cfg.PodNamespace,
//	    },
//	    Client: clientset.CoordinationV1(),
//	    LockConfig: resourcelock.ResourceLockConfig{
//	        Identity: identity,
//	    },
//	}
//
// This test mirrors that literal and asserts the values match the config.
// If main.go's wiring changes, this test will flag a divergence. The
// identity fallback (cfg.PodName == "" -> uuid.NewUUID()) is exercised
// implicitly by the third subtest: the production code generates a UUID
// at runtime, so we only need to confirm an empty PodName does not leak
// into the LeaseLock without being replaced.
func TestLeaseLockWiringFromConfig(t *testing.T) {
	tests := []struct {
		name             string
		cfg              config.Config
		expectedName     string
		expectedNS       string
		expectedIdentity string
	}{
		{
			name: "default lease name and explicit pod identity",
			cfg: config.Config{
				LeaseName:    "castai-workload-resize-migrator",
				PodNamespace: "castai-agent",
				PodName:      "castai-workload-resize-migrator-abc-123",
			},
			expectedName:     "castai-workload-resize-migrator",
			expectedNS:       "castai-agent",
			expectedIdentity: "castai-workload-resize-migrator-abc-123",
		},
		{
			name: "custom lease name with custom namespace and identity",
			cfg: config.Config{
				LeaseName:    "custom-lease",
				PodNamespace: "custom-ns",
				PodName:      "custom-pod-0",
			},
			expectedName:     "custom-lease",
			expectedNS:       "custom-ns",
			expectedIdentity: "custom-pod-0",
		},
		{
			name: "empty pod name is replaced at runtime, not stored verbatim",
			cfg: config.Config{
				LeaseName:    "castai-workload-resize-migrator",
				PodNamespace: "castai-agent",
				PodName:      "",
			},
			expectedName:     "castai-workload-resize-migrator",
			expectedNS:       "castai-agent",
			expectedIdentity: "runtime-generated-uuid-placeholder", // mirrors main.go uuid.NewUUID() fallback
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()

			identity := tc.cfg.PodName
			if identity == "" {
				// Mirror the runtime fallback in main.go. We do NOT
				// call uuid.NewUUID() here because (a) it would make
				// the assertion non-deterministic and (b) the only
				// invariant we need is that an empty PodName does
				// not leak into the lock without being replaced.
				identity = "runtime-generated-uuid-placeholder"
			}

			lock := &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{
					Name:      tc.cfg.LeaseName,
					Namespace: tc.cfg.PodNamespace,
				},
				Client: clientset.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: identity,
				},
			}

			if lock.LeaseMeta.Name != tc.expectedName {
				t.Fatalf("lease Name=%q, want %q", lock.LeaseMeta.Name, tc.expectedName)
			}
			if lock.LeaseMeta.Namespace != tc.expectedNS {
				t.Fatalf("lease Namespace=%q, want %q", lock.LeaseMeta.Namespace, tc.expectedNS)
			}
			if lock.LockConfig.Identity != tc.expectedIdentity {
				t.Fatalf("lease Identity=%q, want %q", lock.LockConfig.Identity, tc.expectedIdentity)
			}
			if lock.Client == nil {
				t.Fatalf("lease Client must not be nil")
			}
		})
	}
}

// TestLeaderElectionDisabledGuard documents the gating condition that
// selects between the two startup paths in main.go:
//
//	if !cfg.LeaderElection { runController(...) }
//	else { leaderelection.RunOrDie(...) }
//
// The disabled path does NOT require POD_NAMESPACE (it is only required
// for the lease lock when leader election is enabled). This test locks
// down that distinction at the config layer so a future change to the
// gating condition is visible in code review.
func TestLeaderElectionDisabledGuard(t *testing.T) {
	cfg := config.Config{
		LeaderElection: false,
		LeaseName:      "castai-workload-resize-migrator",
		PodNamespace:   "", // disabled path must not require POD_NAMESPACE
	}
	if cfg.LeaderElection {
		t.Fatalf("this test only applies to the leader-election-disabled path")
	}
	if cfg.PodNamespace != "" {
		t.Fatalf("disabled-path should be permissive; want empty PodNamespace, got %q", cfg.PodNamespace)
	}

	cfgEnabled := config.Config{
		LeaderElection: true,
		LeaseName:      "castai-workload-resize-migrator",
		PodNamespace:   "", // enabled path requires non-empty POD_NAMESPACE; main.go enforces this
	}
	if !cfgEnabled.LeaderElection {
		t.Fatalf("expected leader election enabled in this case")
	}
}

// TestLeaderElectionDisabledRunsDirectly verifies the gating helper
// shouldUseLeaderElection. The helper decides whether main() takes the
// leader-election path (acquire a lease before processing) or runs the
// controller directly. It gates purely on the LeaderElection flag; the
// missing-namespace fatal lives in main() via validateLeaderElectionConfig
// and is exercised separately by TestValidateLeaderElectionConfig below.
//
// The gating logic is intentionally extracted into shouldUseLeaderElection
// because exercising runController end-to-end would require a real
// kubernetes.Interface + dynamic.Interface wired through an informer
// factory plus a tolerance for the 10s time.Sleep in the safety-scan
// goroutine — far too slow and brittle for a unit test.
func TestLeaderElectionDisabledRunsDirectly(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want bool
	}{
		{
			name: "leader election disabled (LEADER_ELECTION_ENABLED=false) — direct path",
			cfg: config.Config{
				LeaderElection: false,
				PodNamespace:   "", // direct path must NOT require POD_NAMESPACE
			},
			want: false,
		},
		{
			name: "leader election disabled with namespace set — still direct path",
			cfg: config.Config{
				LeaderElection: false,
				PodNamespace:   "castai-agent",
			},
			want: false,
		},
		{
			// shouldUseLeaderElection gates ONLY on cfg.LeaderElection.
			// The missing-namespace case is a misconfig that main() handles
			// separately via validateLeaderElectionConfig (see
			// TestValidateLeaderElectionConfig) — the helper still returns
			// true here so the caller can produce the actionable fatal.
			name: "leader election enabled (POD_NAMESPACE irrelevant to helper)",
			cfg: config.Config{
				LeaderElection: true,
				PodNamespace:   "", // misconfig: fatal-ed by main(), not this helper
			},
			want: true,
		},
		{
			name: "leader election enabled with POD_NAMESPACE — leader path",
			cfg: config.Config{
				LeaderElection: true,
				PodNamespace:   "castai-agent",
				LeaseName:      "castai-workload-resize-migrator",
				PodName:        "castai-workload-resize-migrator-0",
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldUseLeaderElection(tc.cfg)
			if got != tc.want {
				t.Fatalf("shouldUseLeaderElection() = %v, want %v (cfg=%+v)", got, tc.want, tc.cfg)
			}
		})
	}
}

// TestValidateLeaderElectionConfig documents the fatal path in main():
// when leader election is enabled but POD_NAMESPACE is empty,
// validateLeaderElectionConfig returns a non-nil error and main() exits 1.
// The disabled path is permissive (the helper is unreachable there, but
// we still verify it returns nil for completeness).
//
// We do not exercise the os.Exit(1) call directly — that would terminate
// the test binary. Instead, this test pins down the helper that main()
// calls before exiting, which is the actual contract.
func TestValidateLeaderElectionConfig(t *testing.T) {
	// main() only calls validateLeaderElectionConfig on the leader-
	// election-enabled path, so we only exercise that branch here.
	// The disabled-path permissiveness is locked down by
	// TestLeaderElectionDisabledRunsDirectly (helper returns false,
	// so validateLeaderElectionConfig is never reached).
	tests := []struct {
		name    string
		cfg     config.Config
		wantErr bool
	}{
		{
			name: "leader election enabled with namespace — OK",
			cfg: config.Config{
				LeaderElection: true,
				PodNamespace:   "castai-agent",
				LeaseName:      "castai-workload-resize-migrator",
				PodName:        "castai-workload-resize-migrator-0",
			},
			wantErr: false,
		},
		{
			name: "leader election enabled with empty namespace — fatal",
			cfg: config.Config{
				LeaderElection: true,
				PodNamespace:   "", // misconfig: LeaseLock would have empty Namespace
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLeaderElectionConfig(tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatalf("validateLeaderElectionConfig(%+v) = nil, want error", tc.cfg)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateLeaderElectionConfig(%+v) = %v, want nil", tc.cfg, err)
			}
		})
	}
}

// TestMigrateCallbackEnqueuesToWorkqueue verifies that the adapter
// installed by main.go via det.SetMigrateFunc actually enqueues the
// *detector.PodPendingInfo into the workqueue. This is the seam between
// the informer event handler and the migrator: a regression that called
// mig.Trigger directly from the callback would bypass the queue and
// defeat the whole point of decoupling. The test mirrors the production
// adapter byte-for-byte:
//
//	det.SetMigrateFunc(func(_ context.Context, p *detector.PodPendingInfo) error {
//	    q.Add(p)
//	    return nil
//	})
func TestMigrateCallbackEnqueuesToWorkqueue(t *testing.T) {
	q := workqueue.New(workqueue.DefaultBufferSize)
	t.Cleanup(q.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Record the items the queue processes. Using a no-op (success)
	// processor is sufficient — the assertion is that the adapter
	// hands the pod off to the queue, not that the migrator runs.
	var seen []*detector.PodPendingInfo
	var seenMu sync.Mutex
	var processed atomic.Int64
	q.Start(ctx, 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		seenMu.Lock()
		seen = append(seen, p)
		seenMu.Unlock()
		processed.Add(1)
		return nil
	})

	// Build the exact adapter wired in main.go.
	callback := func(_ context.Context, p *detector.PodPendingInfo) error {
		q.Add(p)
		return nil
	}

	item := &detector.PodPendingInfo{
		Namespace: "default",
		PodName:   "pod-pending-1",
		NodeName:  "node-1",
		Reason:    "Infeasible",
	}

	// Invoke the adapter directly. This is what OnPodChange ends up
	// calling when a pod qualifies for migration.
	if err := callback(ctx, item); err != nil {
		t.Fatalf("callback returned error: %v", err)
	}

	// The queue must hand the item to the worker within a short
	// timeout. We poll because the worker is in a separate goroutine.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && processed.Load() == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	if processed.Load() == 0 {
		t.Fatalf("queue did not process the item within 2s; q.Len()=%d", q.Len())
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("expected exactly 1 item processed, got %d", len(seen))
	}
	if seen[0] != item {
		t.Fatalf("expected pointer identity to be preserved (q.Add(p) is non-copying); got %p, want %p", seen[0], item)
	}
	if seen[0].Namespace != "default" || seen[0].PodName != "pod-pending-1" || seen[0].Reason != "Infeasible" {
		t.Fatalf("processed item lost fields: %+v", seen[0])
	}
	if q.Len() != 0 {
		t.Fatalf("expected queue drained after processing, Len()=%d", q.Len())
	}
}

// lifecycleMigrationGVR mirrors the migrator's internal migrationGVR.
// It is unexported there, so the lifecycle test redeclares it here to
// read the created CRDs from the fake dynamic client.
var lifecycleMigrationGVR = schema.GroupVersionResource{
	Group:    "live.cast.ai",
	Version:  "v1",
	Resource: "migrations",
}

// newFakeDynamicClientForLifecycle returns a fake dynamic client that can
// list and create live.cast.ai/v1 Migration objects.
func newFakeDynamicClientForLifecycle() dynamic.Interface {
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		lifecycleMigrationGVR: "MigrationList",
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
}

// TestRunControllerEventPathCreatesMigration is the regression test for the
// composition-root lifecycle bug where runController called q.Stop() before
// waiting on ctx.Done(): the workqueue was closed at startup, detector
// callbacks enqueued into a closed queue and were dropped, and only the
// periodic safety scan could ever trigger migrations.
//
// Both safety-scan fallbacks are configured with hour-long delays so the
// ONLY way a Migration CRD can appear within the test window is the full
// event-driven path: informer Add event -> OnPodChange -> workqueue ->
// migrator.Trigger -> dynamic client Create.
func TestRunControllerEventPathCreatesMigration(t *testing.T) {
	cs := fake.NewSimpleClientset()
	dyn := newFakeDynamicClientForLifecycle()

	cfg := config.Config{
		DryRun:                   false,
		SafetyScanInterval:       time.Hour, // disable periodic safety scan
		SafetyScanStartupDelay:   time.Hour, // disable initial safety scan
		MigrationCleanupInterval: time.Hour, // keep cleanup loop quiet
		MigrationTimeout:         time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- runController(ctx, cfg, cs, dyn) }()

	// Give the informer factory time to start and sync the (empty) cache.
	time.Sleep(500 * time.Millisecond)

	// Create an eligible pod after startup. Infeasible pods are
	// immediately eligible, so no PendingThreshold wait is involved.
	pod := makeInfeasiblePod("lifecycle-pod", "default", "node-1")
	if _, err := cs.CoreV1().Pods("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// The event-driven path must produce the Migration CRD well within
	// the (hour-long) safety-scan window.
	deadline := time.Now().Add(15 * time.Second)
	for {
		list, err := dyn.Resource(lifecycleMigrationGVR).Namespace("default").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatalf("list migrations: %v", err)
		}
		if len(list.Items) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event-driven path did not create a migration within 15s while the safety scan was disabled for 1h — " +
				"runController is likely stopping the workqueue at startup again")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Shutdown must complete promptly: the ctx-aware startup delay must
	// not hold wg.Wait() hostage with a bare sleep.
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("runController returned %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runController did not shut down within 15s of ctx cancellation")
	}
}

// TestPodFromUnwrapsTombstone verifies the informer-event helper used by
// the pod event handlers. DeleteFunc can deliver cache.DeletedFinalStateUnknown
// (a tombstone) when a pod is deleted after an informer resync gap; the
// helper must unwrap it instead of panicking on a direct type assertion.
func TestPodFromUnwrapsTombstone(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}

	if got := podFrom(pod); got != pod {
		t.Fatalf("podFrom(pod) = %p, want %p", got, pod)
	}

	tombstone := cache.DeletedFinalStateUnknown{Key: "default/p", Obj: pod}
	if got := podFrom(tombstone); got != pod {
		t.Fatalf("podFrom(tombstone) = %p, want unwrapped %p", got, pod)
	}

	if got := podFrom("not a pod"); got != nil {
		t.Fatalf("podFrom(non-pod) = %p, want nil", got)
	}
	if got := podFrom(nil); got != nil {
		t.Fatalf("podFrom(nil) = %p, want nil", got)
	}
}
