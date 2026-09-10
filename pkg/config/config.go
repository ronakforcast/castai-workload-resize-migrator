package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"
)

// Config holds controller configuration loaded from environment variables.
// The controller detects pods whose in-place CPU upsize cannot be applied
// because the node is full, and creates CAST AI Container Live Migration
// (CLM) Migration CRDs to move them to nodes where the resize can succeed.
type Config struct {
	DryRun                   bool
	PendingThreshold         time.Duration
	SafetyScanInterval       time.Duration
	SafetyScanStartupDelay   time.Duration
	MigrationTimeout         time.Duration
	MigrationCleanupInterval time.Duration
	MigrationRetryLimit      int
	MigrationRetryDelay      time.Duration
	CLMNodeTemplate          string
	LeaderElection           bool
	LeaseName                string
	PodNamespace             string
	PodName                  string
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		DryRun:                   getBool("DRY_RUN", true),
		PendingThreshold:         getDuration("PENDING_THRESHOLD", 2*time.Minute),
		SafetyScanInterval:       getDuration("SAFETY_SCAN_INTERVAL", 2*time.Minute),
		SafetyScanStartupDelay:   getDuration("SAFETY_SCAN_STARTUP_DELAY", 10*time.Second),
		MigrationTimeout:         getPositiveDuration("MIGRATION_TIMEOUT", 10*time.Minute),
		MigrationCleanupInterval: getDuration("MIGRATION_CLEANUP_INTERVAL", 30*time.Second),
		MigrationRetryLimit:      getInt("MIGRATION_RETRY_LIMIT", 3),
		MigrationRetryDelay:      getDuration("MIGRATION_RETRY_DELAY", 30*time.Second),
		CLMNodeTemplate:          getString("CLM_NODE_TEMPLATE", "clm-live-migration-template"),
		LeaderElection:           getBool("LEADER_ELECTION_ENABLED", true),
		LeaseName:                getString("LEADER_ELECTION_LEASE_NAME", "castai-workload-resize-migrator"),
		PodNamespace:             getString("POD_NAMESPACE", ""),
		PodName:                  getString("POD_NAME", ""),
	}
}

func getString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		slog.Warn("invalid boolean value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback,
			"error", err.Error(),
		)
		return fallback
	}
	return b
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback.String(),
			"error", err.Error(),
		)
		return fallback
	}
	return d
}

// getPositiveDuration is like getDuration but additionally falls back to
// the default when the parsed value is zero or negative. Used for
// durations where a non-positive value would silently disable a safety
// mechanism (e.g. MIGRATION_TIMEOUT: zero would track stuck migrations
// forever). Issue #1, item F8.
func getPositiveDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback.String(),
			"error", err.Error(),
		)
		return fallback
	}
	if d <= 0 {
		slog.Warn("non-positive duration value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback.String(),
		)
		return fallback
	}
	return d
}

func getFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		slog.Warn("invalid float value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback,
			"error", err.Error(),
		)
		return fallback
	}
	return f
}

func getInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid int value for env var; falling back to default",
			"key", key,
			"value", v,
			"default", fallback,
			"error", err.Error(),
		)
		return fallback
	}
	return i
}
