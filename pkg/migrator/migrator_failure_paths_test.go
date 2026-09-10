package migrator

// Failure-path characterization tests for the gaps tracked in issue #1
// (section B, items F1–F9):
// https://github.com/ronakforcast/castai-workload-resize-migrator/issues/1
//
// Several of these deliberately assert CURRENT behavior that is a known
// risk rather than ideal behavior. They lock in exactly what the
// controller does today so that (a) unintended changes are caught and
// (b) a future fix for the risk has a precise test to turn around.
// Each test's comment says which parts are intended behavior and which
// are documented gaps awaiting a decision in the issue.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

// F1 (known risk, characterized): migration names are unique per attempt
// and the dedup map is in-memory only. After a controller restart the
// map is empty, so a still-stuck pod gets a SECOND Migration CR even
// while the pre-restart one is in flight on CLM's side. This proves the
// duplicate is created; what the real CLM agent does with two concurrent
// Migrations for one pod is the infra-dependent half of F1.
func TestRestartLosesDedupAndCreatesDuplicateMigration(t *testing.T) {
	client := newFakeDynamicClient()

	// Simulate a Migration left in flight by the "previous controller
	// life" (pre-restart).
	seedMigration(t, client, "default", "mig-inflight", "Running")

	// Fresh Client == post-restart state: empty in-memory map, same cluster.
	c := New(config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}, client)

	if err := c.Trigger(context.Background(), []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	list, err := client.Resource(migrationGVR).Namespace("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 migration CRs after restart (in-flight + duplicate), got %d", len(list.Items))
	}

	// The new entry must track the NEW CR, not adopt the in-flight one.
	c.mu.Lock()
	entry := c.activeMigrations["default/nginx"]
	c.mu.Unlock()
	if entry == nil || entry.migration == "mig-inflight" {
		t.Fatalf("expected the fresh client to track its own new CR, got %+v", entry)
	}
}

// F2 (known risk, characterized): if a tracked Migration CR is deleted
// (manually or by a future GC), getMigrationState fails forever, cleanup
// can never classify the entry, and IsActive stays true permanently —
// the detector skips that pod until the controller restarts. Fix
// decision pending in the issue (e.g. treat persistent NotFound as
// removable).
func TestDeletedMigrationCRLeavesEntryStuckForever(t *testing.T) {
	client := newFakeDynamicClient()
	c := New(config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}, client)
	ctx := context.Background()

	if err := c.Trigger(ctx, []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	c.mu.Lock()
	name := c.activeMigrations["default/nginx"].migration
	c.mu.Unlock()

	// Operator (or GC) deletes the CR behind the controller's back.
	if err := client.Resource(migrationGVR).Namespace("default").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete migration CR: %v", err)
	}

	// Cleanup ticks many times (as it does every interval)…
	for i := 0; i < 5; i++ {
		c.CleanupCompletedMigrations(ctx)
	}

	// …but the entry is permanently stuck: the pod stays invisible to
	// the controller until a restart.
	if !c.IsActive("default", "nginx") {
		t.Fatal("expected entry to remain stuck after CR deletion (known F2 behavior)")
	}
}

// F3 (known risk, characterized): cleanup removes in-memory entries but
// never deletes the Migration CRs themselves. Stale CRs accumulate
// forever; the managed=true label exists for a future GC but nothing
// consumes it yet.
func TestCleanupRemovesTrackingButNeverDeletesMigrationCRs(t *testing.T) {
	client := newFakeDynamicClient()
	c := New(config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}, client)
	ctx := context.Background()

	if err := c.Trigger(ctx, []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	c.mu.Lock()
	name := c.activeMigrations["default/nginx"].migration
	c.mu.Unlock()

	// Migration completes.
	obj, err := client.Resource(migrationGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get migration: %v", err)
	}
	obj.Object["status"] = map[string]interface{}{"state": "Completed"}
	if _, err := client.Resource(migrationGVR).Namespace("default").Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update migration status: %v", err)
	}

	c.CleanupCompletedMigrations(ctx)

	// In-memory tracking is gone…
	if c.IsActive("default", "nginx") {
		t.Fatal("expected entry removed after Completed state")
	}
	// …but the CR is orphaned in the cluster.
	list, err := client.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected the completed Migration CR to remain in the cluster (known F3 behavior), got %d", len(list.Items))
	}
}

// F5 (known risk, characterized): every retry creates a Migration CR
// with a fresh generated name; the previously failed CR is left in
// place. N retries therefore leave N+1 CRs for the same pod (compounds
// F3's accumulation).
func TestRetryCreatesNewMigrationCRWithDifferentName(t *testing.T) {
	client := newFakeDynamicClient()
	seedMigration(t, client, "default", "mig-failed", "Failed")

	c := New(config.Config{
		DryRun:              false,
		MigrationRetryLimit: 3,
		MigrationRetryDelay: 0,
		MigrationTimeout:    10 * time.Minute,
	}, client)

	// White-box seed the tracked entry so the retry path runs against
	// the already-failed CR (same pattern as the race test).
	c.mu.Lock()
	c.activeMigrations["default/nginx"] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now().Add(-time.Hour),
		migration:  "mig-failed",
		retryCount: 0,
	}
	c.mu.Unlock()

	if err := c.Trigger(context.Background(), []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	list, err := client.Resource(migrationGVR).Namespace("default").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected old failed CR + new retry CR (known F5 behavior), got %d", len(list.Items))
	}

	c.mu.Lock()
	entry := c.activeMigrations["default/nginx"]
	c.mu.Unlock()
	if entry == nil || entry.migration == "mig-failed" {
		t.Fatalf("expected entry to track the new retry CR, got %+v", entry)
	}
}

// F6 (intended behavior, locked in): when the live.cast.ai CRD is
// missing (CLM not installed), creates fail with a NotFound-class error.
// The controller must not crash or poison state — and it must recover
// automatically once the CRD appears (first two creates fail, the third
// succeeds).
func TestCRDMissingErrorsThenRecoversWithoutCrash(t *testing.T) {
	client := newFakeDynamicClient()

	var calls atomic.Int32
	client.PrependReactor("create", "migrations", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) <= 2 {
			return true, nil, apierrors.NewNotFound(migrationGVR.GroupResource(), "migrations")
		}
		return false, nil, nil // fall through: CRD "installed" from now on
	})

	c := New(config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}, client)
	ctx := context.Background()
	pods := []*detector.PodPendingInfo{samplePod()}

	// Two failed attempts: errors surfaced, no panic, no entry recorded.
	if err := c.Trigger(ctx, pods); err == nil {
		t.Fatal("expected error while CRD missing")
	}
	if c.IsActive("default", "nginx") {
		t.Fatal("no entry should be recorded on failed create")
	}
	if err := c.Trigger(ctx, pods); err == nil {
		t.Fatal("expected error while CRD missing (second attempt)")
	}

	// CRD appears: the very next trigger succeeds.
	if err := c.Trigger(ctx, pods); err != nil {
		t.Fatalf("expected recovery once CRD exists, got %v", err)
	}
	if !c.IsActive("default", "nginx") {
		t.Fatal("expected entry after successful create")
	}

	list, err := client.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one migration CR after recovery, got %d", len(list.Items))
	}
}

// F8 (known risk, characterized): MigrationTimeout=0 explicitly
// disables the wall-clock timeout, so a migration stuck in a
// non-terminal state is tracked forever (memory entry + permanent
// IsActive block on the pod). The config value is operator-supplied;
// this documents the consequence of the zero value.
func TestZeroMigrationTimeoutDisablesExpiry(t *testing.T) {
	client := newFakeDynamicClient()
	seedMigration(t, client, "default", "mig-stuck", "Running")

	c := New(config.Config{DryRun: false, MigrationTimeout: 0}, client)

	c.mu.Lock()
	c.activeMigrations["default/nginx"] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now().Add(-24 * time.Hour),
		migration:  "mig-stuck",
		retryCount: 0,
	}
	c.mu.Unlock()

	c.CleanupCompletedMigrations(context.Background())

	if !c.IsActive("default", "nginx") {
		t.Fatal("expected entry to stay tracked with MigrationTimeout=0 (known F8 behavior)")
	}
}

// F9 (known risk, characterized): only Completed/Succeeded/Failed are
// recognized. A terminal-but-unknown state such as "Canceled" (per the
// CLM changelog, cancellation is now a distinct recorded outcome) is
// treated as non-terminal, and the entry is rescued only by the
// wall-clock timeout.
func TestUnknownTerminalStateRescuedOnlyByTimeout(t *testing.T) {
	client := newFakeDynamicClient()
	seedMigration(t, client, "default", "mig-canceled", "Canceled")

	c := New(config.Config{DryRun: false, MigrationTimeout: time.Hour}, client)

	c.mu.Lock()
	c.activeMigrations["default/nginx"] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now(),
		migration:  "mig-canceled",
		retryCount: 0,
	}
	c.mu.Unlock()

	ctx := context.Background()

	// Fresh entry within the timeout: unknown state is not terminal.
	c.CleanupCompletedMigrations(ctx)
	if !c.IsActive("default", "nginx") {
		t.Fatal("unknown state within timeout should stay tracked")
	}

	// Once the wall-clock timeout passes, the entry is finally removed.
	c.mu.Lock()
	c.activeMigrations["default/nginx"].createdAt = time.Now().Add(-2 * time.Hour)
	c.mu.Unlock()
	c.CleanupCompletedMigrations(ctx)
	if c.IsActive("default", "nginx") {
		t.Fatal("expected timeout to rescue the unknown-state entry")
	}
}
