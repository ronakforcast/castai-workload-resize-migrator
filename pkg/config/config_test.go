package config

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// captureSlog redirects the default slog logger to an in-memory buffer
// for the duration of the test. The returned restore function must be
// deferred to put the previous logger back.
func captureSlog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	prev := slog.Default()
	buf := &bytes.Buffer{}
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(handler))
	return buf, func() { slog.SetDefault(prev) }
}

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{
		"DRY_RUN", "PENDING_THRESHOLD",
		"SAFETY_SCAN_INTERVAL", "SAFETY_SCAN_STARTUP_DELAY",
		"MIGRATION_TIMEOUT", "MIGRATION_CLEANUP_INTERVAL", "MIGRATION_RETRY_LIMIT",
		"MIGRATION_RETRY_DELAY", "CLM_NODE_TEMPLATE",
		"LEADER_ELECTION_ENABLED", "LEADER_ELECTION_LEASE_NAME",
		"POD_NAMESPACE", "POD_NAME",
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
	if cfg.SafetyScanStartupDelay != 10*time.Second {
		t.Fatalf("expected SafetyScanStartupDelay=10s by default, got %v", cfg.SafetyScanStartupDelay)
	}
	if cfg.MigrationTimeout != 10*time.Minute {
		t.Fatalf("expected MigrationTimeout=10m by default, got %v", cfg.MigrationTimeout)
	}
	if cfg.MigrationCleanupInterval != 30*time.Second {
		t.Fatalf("expected MigrationCleanupInterval=30s by default, got %v", cfg.MigrationCleanupInterval)
	}
	if cfg.MigrationRetryLimit != 3 {
		t.Fatalf("expected MigrationRetryLimit=3 by default, got %v", cfg.MigrationRetryLimit)
	}
	if cfg.MigrationRetryDelay != 30*time.Second {
		t.Fatalf("expected MigrationRetryDelay=30s by default, got %v", cfg.MigrationRetryDelay)
	}
	if cfg.CLMNodeTemplate != "clm-live-migration-template" {
		t.Fatalf("expected CLMNodeTemplate=clm-live-migration-template by default, got %v", cfg.CLMNodeTemplate)
	}
	if cfg.LeaderElection != true {
		t.Fatalf("expected LeaderElection=true by default, got %v", cfg.LeaderElection)
	}
	if cfg.LeaseName != "castai-workload-resize-migrator" {
		t.Fatalf("expected LeaseName=castai-workload-resize-migrator by default, got %v", cfg.LeaseName)
	}
	if cfg.PodNamespace != "" {
		t.Fatalf("expected PodNamespace=\"\" by default, got %v", cfg.PodNamespace)
	}
	if cfg.PodName != "" {
		t.Fatalf("expected PodName=\"\" by default, got %v", cfg.PodName)
	}
}

func TestLoadOverrides(t *testing.T) {
	setenvs := map[string]string{
		"DRY_RUN":                    "false",
		"PENDING_THRESHOLD":          "30s",
		"SAFETY_SCAN_INTERVAL":       "3m",
		"SAFETY_SCAN_STARTUP_DELAY":  "5s",
		"MIGRATION_TIMEOUT":          "5m",
		"MIGRATION_CLEANUP_INTERVAL": "45s",
		"MIGRATION_RETRY_LIMIT":      "5",
		"MIGRATION_RETRY_DELAY":      "1m",
		"CLM_NODE_TEMPLATE":          "my-clm-template",
		"LEADER_ELECTION_ENABLED":    "false",
		"LEADER_ELECTION_LEASE_NAME": "test-lease",
		"POD_NAMESPACE":              "test-ns",
		"POD_NAME":                   "test-pod",
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
	if cfg.SafetyScanStartupDelay != 5*time.Second {
		t.Fatalf("expected SafetyScanStartupDelay=5s, got %v", cfg.SafetyScanStartupDelay)
	}
	if cfg.MigrationTimeout != 5*time.Minute {
		t.Fatalf("expected MigrationTimeout=5m, got %v", cfg.MigrationTimeout)
	}
	if cfg.MigrationCleanupInterval != 45*time.Second {
		t.Fatalf("expected MigrationCleanupInterval=45s, got %v", cfg.MigrationCleanupInterval)
	}
	if cfg.MigrationRetryLimit != 5 {
		t.Fatalf("expected MigrationRetryLimit=5, got %v", cfg.MigrationRetryLimit)
	}
	if cfg.MigrationRetryDelay != 1*time.Minute {
		t.Fatalf("expected MigrationRetryDelay=1m, got %v", cfg.MigrationRetryDelay)
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

func TestLoadInvalidSafetyScanStartupDelayFallsBack(t *testing.T) {
	os.Setenv("SAFETY_SCAN_STARTUP_DELAY", "bad")
	defer os.Unsetenv("SAFETY_SCAN_STARTUP_DELAY")

	cfg := Load()
	if cfg.SafetyScanStartupDelay != 10*time.Second {
		t.Fatalf("expected fallback SafetyScanStartupDelay=10s, got %v", cfg.SafetyScanStartupDelay)
	}
}

func TestLoadInvalidMigrationCleanupIntervalFallsBack(t *testing.T) {
	os.Setenv("MIGRATION_CLEANUP_INTERVAL", "bad")
	defer os.Unsetenv("MIGRATION_CLEANUP_INTERVAL")

	cfg := Load()
	if cfg.MigrationCleanupInterval != 30*time.Second {
		t.Fatalf("expected fallback MigrationCleanupInterval=30s, got %v", cfg.MigrationCleanupInterval)
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

// F8 fix (issue #1): a zero MIGRATION_TIMEOUT would silently disable the
// stuck-migration timeout and track entries forever. Non-positive values
// must fall back to the default.
func TestLoadZeroMigrationTimeoutFallsBack(t *testing.T) {
	os.Setenv("MIGRATION_TIMEOUT", "0")
	defer os.Unsetenv("MIGRATION_TIMEOUT")

	cfg := Load()
	if cfg.MigrationTimeout != 10*time.Minute {
		t.Fatalf("expected fallback MigrationTimeout=10m for zero value, got %v", cfg.MigrationTimeout)
	}
}

func TestLoadNegativeMigrationTimeoutFallsBack(t *testing.T) {
	os.Setenv("MIGRATION_TIMEOUT", "-5m")
	defer os.Unsetenv("MIGRATION_TIMEOUT")

	cfg := Load()
	if cfg.MigrationTimeout != 10*time.Minute {
		t.Fatalf("expected fallback MigrationTimeout=10m for negative value, got %v", cfg.MigrationTimeout)
	}
}

func TestLoadInvalidDurationLogsWarning(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	os.Setenv("PENDING_THRESHOLD", "not-a-duration")
	defer os.Unsetenv("PENDING_THRESHOLD")

	cfg := Load()
	if cfg.PendingThreshold != 2*time.Minute {
		t.Fatalf("expected fallback PendingThreshold=2m, got %v", cfg.PendingThreshold)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected WARN log line, got: %s", out)
	}
	if !strings.Contains(out, "PENDING_THRESHOLD") {
		t.Fatalf("expected log to contain key PENDING_THRESHOLD, got: %s", out)
	}
	if !strings.Contains(out, "not-a-duration") {
		t.Fatalf("expected log to contain invalid value not-a-duration, got: %s", out)
	}
}

func TestLoadInvalidBoolLogsWarning(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	os.Setenv("DRY_RUN", "maybe")
	defer os.Unsetenv("DRY_RUN")

	cfg := Load()
	if cfg.DryRun != true {
		t.Fatalf("expected fallback DryRun=true, got %v", cfg.DryRun)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected WARN log line, got: %s", out)
	}
	if !strings.Contains(out, "DRY_RUN") {
		t.Fatalf("expected log to contain key DRY_RUN, got: %s", out)
	}
	if !strings.Contains(out, "maybe") {
		t.Fatalf("expected log to contain invalid value maybe, got: %s", out)
	}
}

func TestLoadInvalidIntLogsWarning(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	os.Setenv("MIGRATION_RETRY_LIMIT", "abc")
	defer os.Unsetenv("MIGRATION_RETRY_LIMIT")

	cfg := Load()
	if cfg.MigrationRetryLimit != 3 {
		t.Fatalf("expected fallback MigrationRetryLimit=3, got %v", cfg.MigrationRetryLimit)
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected WARN log line, got: %s", out)
	}
	if !strings.Contains(out, "MIGRATION_RETRY_LIMIT") {
		t.Fatalf("expected log to contain key MIGRATION_RETRY_LIMIT, got: %s", out)
	}
	if !strings.Contains(out, "abc") {
		t.Fatalf("expected log to contain invalid value abc, got: %s", out)
	}
}
