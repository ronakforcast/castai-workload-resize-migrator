package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{
		"DRY_RUN", "PENDING_THRESHOLD",
		"SAFETY_SCAN_INTERVAL", "MIGRATION_TIMEOUT", "MIGRATION_RETRY_LIMIT",
		"MIGRATION_RETRY_DELAY", "MIGRATION_ALERT_THRESHOLD", "CLM_NODE_TEMPLATE",
	} {
		os.Unsetenv(k)
	}

	cfg := Load()

	if cfg.DryRun != true {
		t.Fatalf("expected DryRun=true by default, got %v", cfg.DryRun)
	}
	if cfg.PendingThreshold != 2*time.Minute {
		t.Fatalf("expected PendingThreshold=2m by default, got %v", cfg.PendingThreshold)
	}
	if cfg.SafetyScanInterval != 2*time.Minute {
		t.Fatalf("expected SafetyScanInterval=2m by default, got %v", cfg.SafetyScanInterval)
	}
	if cfg.MigrationTimeout != 10*time.Minute {
		t.Fatalf("expected MigrationTimeout=10m by default, got %v", cfg.MigrationTimeout)
	}
	if cfg.MigrationRetryLimit != 3 {
		t.Fatalf("expected MigrationRetryLimit=3 by default, got %v", cfg.MigrationRetryLimit)
	}
	if cfg.MigrationRetryDelay != 30*time.Second {
		t.Fatalf("expected MigrationRetryDelay=30s by default, got %v", cfg.MigrationRetryDelay)
	}
	if cfg.MigrationAlertThreshold != 3 {
		t.Fatalf("expected MigrationAlertThreshold=3 by default, got %v", cfg.MigrationAlertThreshold)
	}
	if cfg.CLMNodeTemplate != "clm-live-migration-template" {
		t.Fatalf("expected CLMNodeTemplate=clm-live-migration-template by default, got %v", cfg.CLMNodeTemplate)
	}
}

func TestLoadOverrides(t *testing.T) {
	setenvs := map[string]string{
		"DRY_RUN":                    "false",
		"PENDING_THRESHOLD":           "30s",
		"SAFETY_SCAN_INTERVAL":        "3m",
		"MIGRATION_TIMEOUT":           "5m",
		"MIGRATION_RETRY_LIMIT":       "5",
		"MIGRATION_RETRY_DELAY":       "1m",
		"MIGRATION_ALERT_THRESHOLD":   "10",
		"CLM_NODE_TEMPLATE":           "my-clm-template",
		"LEADER_ELECTION_ENABLED":     "false",
		"LEADER_ELECTION_LEASE_NAME":  "test-lease",
		"POD_NAMESPACE":               "test-ns",
		"POD_NAME":                    "test-pod",
	}
	for k, v := range setenvs {
		os.Setenv(k, v)
		defer os.Unsetenv(k)
	}

	cfg := Load()

	if cfg.DryRun != false {
		t.Fatalf("expected DryRun=false, got %v", cfg.DryRun)
	}
	if cfg.PendingThreshold != 30*time.Second {
		t.Fatalf("expected PendingThreshold=30s, got %v", cfg.PendingThreshold)
	}
	if cfg.SafetyScanInterval != 3*time.Minute {
		t.Fatalf("expected SafetyScanInterval=3m, got %v", cfg.SafetyScanInterval)
	}
	if cfg.MigrationTimeout != 5*time.Minute {
		t.Fatalf("expected MigrationTimeout=5m, got %v", cfg.MigrationTimeout)
	}
	if cfg.MigrationRetryLimit != 5 {
		t.Fatalf("expected MigrationRetryLimit=5, got %v", cfg.MigrationRetryLimit)
	}
	if cfg.MigrationRetryDelay != 1*time.Minute {
		t.Fatalf("expected MigrationRetryDelay=1m, got %v", cfg.MigrationRetryDelay)
	}
	if cfg.MigrationAlertThreshold != 10 {
		t.Fatalf("expected MigrationAlertThreshold=10, got %v", cfg.MigrationAlertThreshold)
	}
	if cfg.CLMNodeTemplate != "my-clm-template" {
		t.Fatalf("expected CLMNodeTemplate=my-clm-template, got %v", cfg.CLMNodeTemplate)
	}
	if cfg.LeaderElection != false {
		t.Fatalf("expected LeaderElection=false, got %v", cfg.LeaderElection)
	}
	if cfg.LeaseName != "test-lease" {
		t.Fatalf("expected LeaseName=test-lease, got %v", cfg.LeaseName)
	}
	if cfg.PodNamespace != "test-ns" {
		t.Fatalf("expected PodNamespace=test-ns, got %v", cfg.PodNamespace)
	}
	if cfg.PodName != "test-pod" {
		t.Fatalf("expected PodName=test-pod, got %v", cfg.PodName)
	}
}

func TestLoadInvalidDurationFallsBack(t *testing.T) {
	os.Setenv("PENDING_THRESHOLD", "not-a-duration")
	defer os.Unsetenv("PENDING_THRESHOLD")

	cfg := Load()
	if cfg.PendingThreshold != 2*time.Minute {
		t.Fatalf("expected fallback PendingThreshold=2m, got %v", cfg.PendingThreshold)
	}
}

func TestLoadInvalidBoolFallsBack(t *testing.T) {
	os.Setenv("DRY_RUN", "maybe")
	defer os.Unsetenv("DRY_RUN")

	cfg := Load()
	if cfg.DryRun != true {
		t.Fatalf("expected fallback DryRun=true, got %v", cfg.DryRun)
	}
}

func TestLoadInvalidIntFallsBack(t *testing.T) {
	os.Setenv("MIGRATION_RETRY_LIMIT", "abc")
	defer os.Unsetenv("MIGRATION_RETRY_LIMIT")

	cfg := Load()
	if cfg.MigrationRetryLimit != 3 {
		t.Fatalf("expected fallback MigrationRetryLimit=3, got %v", cfg.MigrationRetryLimit)
	}
}

func TestLoadInvalidSafetyScanIntervalFallsBack(t *testing.T) {
	os.Setenv("SAFETY_SCAN_INTERVAL", "bad")
	defer os.Unsetenv("SAFETY_SCAN_INTERVAL")

	cfg := Load()
	if cfg.SafetyScanInterval != 2*time.Minute {
		t.Fatalf("expected fallback SafetyScanInterval=2m, got %v", cfg.SafetyScanInterval)
	}
}

func TestLoadInvalidMigrationRetryDelayFallsBack(t *testing.T) {
	os.Setenv("MIGRATION_RETRY_DELAY", "bad")
	defer os.Unsetenv("MIGRATION_RETRY_DELAY")

	cfg := Load()
	if cfg.MigrationRetryDelay != 30*time.Second {
		t.Fatalf("expected fallback MigrationRetryDelay=30s, got %v", cfg.MigrationRetryDelay)
	}
}
