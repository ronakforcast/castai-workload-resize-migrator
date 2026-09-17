package migrator

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func pendingPod(name, node string) *detector.PodPendingInfo {
	return &detector.PodPendingInfo{
		Namespace:    "default",
		PodName:      name,
		NodeName:     node,
		AllocatedCPU: 500,
		DesiredCPU:   2000,
	}
}

// TestCircuitBreakerRateLimit verifies the per-pod hourly cap: once the
// limit is reached, createMigration refuses and the migration count in the
// fake cluster stops growing.
func TestCircuitBreakerRateLimit(t *testing.T) {
	cfg := config.Config{MigrationRateLimitPerHour: 2, MigrationRetryLimit: 3}
	m := newFakeMigrator(t, cfg)

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := m.createMigration(ctx, pendingPod("app", "src"), 0); err != nil {
			t.Fatalf("migration %d should be allowed: %v", i, err)
		}
	}
	if err := m.createMigration(ctx, pendingPod("app", "src"), 0); err == nil {
		t.Fatal("third migration within the hour must be rejected by the circuit breaker")
	}

	list, err := m.client.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected exactly 2 migration CRs, got %d", len(list.Items))
	}

	// A different pod is unaffected by the first pod's budget.
	if err := m.createMigration(ctx, pendingPod("other", "src"), 0); err != nil {
		t.Fatalf("different pod must have its own budget: %v", err)
	}
}

// TestCircuitBreakerConcurrentCap verifies the global concurrency cap: new
// migrations are deferred while the number of tracked entries is at the
// cap. Retries of tracked pods are exempt.
func TestCircuitBreakerConcurrentCap(t *testing.T) {
	cfg := config.Config{MaxConcurrentMigrations: 1, MigrationRetryLimit: 3}
	m := newFakeMigrator(t, cfg)

	ctx := context.Background()
	if err := m.createMigration(ctx, pendingPod("app", "src"), 0); err != nil {
		t.Fatalf("first migration should be allowed: %v", err)
	}
	if err := m.createMigration(ctx, pendingPod("other", "src"), 0); err == nil {
		t.Fatal("second concurrent migration must be deferred (cap reached)")
	}
	// Retries of the tracked pod are exempt from the cap.
	if err := m.createMigration(ctx, pendingPod("app", "src"), 1); err != nil {
		t.Fatalf("retry of tracked pod must bypass the concurrency cap: %v", err)
	}
}

// TestFailedDestinationExcludedAfterExhaustion verifies that once a pod's
// retry limit is exhausted on a destination, the next cycle's destination
// selection skips that node and picks a different one.
func TestFailedDestinationExcludedAfterExhaustion(t *testing.T) {
	cfg := config.Config{
		MigrationRetryLimit:       3,
		FailedDestinationTTL:      1 * time.Hour,
		CLMNodeTemplate:           "clm-live-migration-template",
		MaxConcurrentMigrations:   0,
		MigrationRateLimitPerHour: 0,
	}
	m := newFakeMigrator(t, cfg)
	// A second destination node (uniquely named; the default seeds already
	// own dest-node-1/2) so an alternative exists after exclusion.
	extra := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]interface{}{
			"name": "clm-second-dest",
			"labels": map[string]interface{}{
				"live.cast.ai/migration-enabled":   "true",
				"scheduling.cast.ai/node-template": "clm-live-migration-template",
			},
		},
	}}
	if _, err := m.client.Resource(nodeGVR).Create(context.Background(), extra, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed extra destination: %v", err)
	}

	ctx := context.Background()
	if err := m.createMigration(ctx, pendingPod("app", "src"), 0); err != nil {
		t.Fatalf("first migration: %v", err)
	}
	first, err := m.selectDestinationNode(ctx, "src", "default/app")
	if err != nil {
		t.Fatal(err)
	}

	// Simulate give-up on that destination.
	m.mu.Lock()
	m.failedDestinations["default/app"] = failedDestination{node: first, until: time.Now().Add(time.Hour)}
	m.mu.Unlock()

	second, err := m.selectDestinationNode(ctx, "src", "default/app")
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("failed destination %q must be skipped; got it again", first)
	}

	// After the TTL lapses, the node is eligible again.
	m.mu.Lock()
	m.failedDestinations["default/app"] = failedDestination{node: first, until: time.Now().Add(-time.Minute)}
	m.mu.Unlock()
	again, err := m.selectDestinationNode(ctx, "src", "default/app")
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("after TTL the original destination must be re-selectable; got %q want %q", again, first)
	}
}

// TestFailedDestinationFallbackNeverStrands verifies the fail-safe: when
// the failed node is the only candidate, it is re-selected rather than
// leaving the pod without any destination.
func TestFailedDestinationFallbackNeverStrands(t *testing.T) {
	cfg := config.Config{
		FailedDestinationTTL: 1 * time.Hour,
		CLMNodeTemplate:      "clm-live-migration-template",
	}
	m := newFakeMigrator(t, cfg)

	ctx := context.Background()
	dest, err := m.selectDestinationNode(ctx, "src", "default/app")
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.failedDestinations["default/app"] = failedDestination{node: dest, until: time.Now().Add(time.Hour)}
	m.mu.Unlock()

	again, err := m.selectDestinationNode(ctx, "src", "default/app")
	if err != nil {
		t.Fatalf("sole candidate must be re-selected as fallback: %v", err)
	}
	if again != dest {
		t.Fatalf("expected fallback to the sole candidate %q, got %q", dest, again)
	}
}
