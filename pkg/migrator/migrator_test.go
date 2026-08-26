package migrator

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
)

func newFakeMigrator(t *testing.T, cfg config.Config) *Client {
	client := fake.NewSimpleDynamicClient(runtime.NewScheme())
	return New(cfg, client)
}

func TestTriggerDryRun(t *testing.T) {
	cfg := config.Config{DryRun: true}
	m := newFakeMigrator(t, cfg)

	pods := []*detector.PodPendingInfo{
		{
			Namespace:    "default",
			PodName:      "nginx",
			NodeName:     "node-1",
			AllocatedCPU: 100,
			DesiredCPU:   400,
		},
	}

	err := m.Trigger(context.Background(), pods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if m.IsActive("default", "nginx") {
		t.Fatal("expected no active migration in dry-run")
	}
}

func TestTriggerCreatesMigration(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	pods := []*detector.PodPendingInfo{
		{
			Namespace:    "default",
			PodName:      "nginx",
			NodeName:     "node-1",
			AllocatedCPU: 100,
			DesiredCPU:   400,
		},
	}

	err := m.Trigger(context.Background(), pods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !m.IsActive("default", "nginx") {
		t.Fatal("expected active migration after trigger")
	}
}

func TestTriggerSkipsActivePod(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	pods := []*detector.PodPendingInfo{
		{
			Namespace:    "default",
			PodName:      "nginx",
			NodeName:     "node-1",
			AllocatedCPU: 100,
			DesiredCPU:   400,
		},
	}

	err := m.Trigger(context.Background(), pods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = m.Trigger(context.Background(), pods)
	if err != nil {
		t.Fatalf("unexpected error on second trigger: %v", err)
	}
}

func TestCleanupExpiredMigrations(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 1 * time.Millisecond}
	m := newFakeMigrator(t, cfg)

	// Manually add an entry that is older than the timeout.
	key := "default/nginx"
	m.activeMigrations[key] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now().Add(-1 * time.Hour),
		migration:  "nginx-woop-123",
		retryCount: 0,
	}

	// cleanupExpiredMigrations is called inside triggerOne / IsActive.
	// Call IsActive to trigger cleanup.
	time.Sleep(5 * time.Millisecond)
	m.IsActive("default", "nginx")

	if m.IsActive("default", "nginx") {
		t.Fatal("expected migration to be cleaned up after timeout")
	}
}

func TestShouldRetryRespectsLimit(t *testing.T) {
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 2,
		MigrationRetryDelay: 1 * time.Millisecond,
	}
	m := newFakeMigrator(t, cfg)

	key := "default/nginx"
	m.activeMigrations[key] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now().Add(-1 * time.Hour),
		migration:  "nginx-woop-123",
		retryCount: 2,
	}

	if m.shouldRetry(context.Background(), key, m.activeMigrations[key]) {
		t.Fatal("expected shouldRetry=false when retry limit reached")
	}
}

func TestShouldRetryRespectsDelay(t *testing.T) {
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 3,
		MigrationRetryDelay: 1 * time.Hour,
	}
	m := newFakeMigrator(t, cfg)

	key := "default/nginx"
	m.activeMigrations[key] = &migrationEntry{
		name:       "nginx",
		namespace:  "default",
		createdAt:  time.Now(),
		migration:  "nginx-woop-123",
		retryCount: 0,
	}

	if m.shouldRetry(context.Background(), key, m.activeMigrations[key]) {
		t.Fatal("expected shouldRetry=false when retry delay not yet passed")
	}
}

func TestTriggerEmptyPodList(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	err := m.Trigger(context.Background(), []*detector.PodPendingInfo{})
	if err != nil {
		t.Fatalf("unexpected error on empty list: %v", err)
	}
}

func TestTriggerMultiplePods(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	pods := []*detector.PodPendingInfo{
		{Namespace: "default", PodName: "pod-a", NodeName: "node-1", AllocatedCPU: 100, DesiredCPU: 400},
		{Namespace: "default", PodName: "pod-b", NodeName: "node-2", AllocatedCPU: 100, DesiredCPU: 400},
		{Namespace: "default", PodName: "pod-c", NodeName: "node-1", AllocatedCPU: 100, DesiredCPU: 400},
	}

	err := m.Trigger(context.Background(), pods)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !m.IsActive("default", "pod-a") {
		t.Fatal("expected pod-a to be active")
	}
	if !m.IsActive("default", "pod-b") {
		t.Fatal("expected pod-b to be active")
	}
	if !m.IsActive("default", "pod-c") {
		t.Fatal("expected pod-c to be active")
	}
}

func TestIsActiveNonExistentPod(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	if m.IsActive("default", "nonexistent") {
		t.Fatal("expected IsActive=false for non-existent pod")
	}
}

func TestGenerateMigrationNameTruncation(t *testing.T) {
	longName := "this-is-a-very-long-pod-name-that-exceeds-fifty-characters-yes-it-does"
	p := &detector.PodPendingInfo{
		Namespace: "default",
		PodName:   longName,
	}
	name := generateMigrationName(p)
	// name = base[:50] + "-woop-" + timestamp = 50 + 6 + 10 = 66 max
	// k8s names can be up to 253 chars, but we want to keep it reasonable
	if len(name) > 67 {
		t.Fatalf("migration name too long: %d chars: %s", len(name), name)
	}
	if name[:50] != longName[:50] {
		t.Fatal("expected first 50 chars of pod name to be used")
	}
}
