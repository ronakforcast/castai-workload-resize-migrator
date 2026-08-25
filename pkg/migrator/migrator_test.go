package migrator

import (
	"context"
	"testing"
	"time"

	"woop-rebalance-controller/pkg/config"
	"woop-rebalance-controller/pkg/detector"

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

func TestCleanupActiveMigrations(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 1 * time.Millisecond}
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

	time.Sleep(5 * time.Millisecond)
	if m.IsActive("default", "nginx") {
		t.Fatal("expected migration to be cleaned up after timeout")
	}
}
