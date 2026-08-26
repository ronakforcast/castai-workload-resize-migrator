package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds controller configuration loaded from environment variables.
// The controller detects pods whose in-place CPU upsize cannot be applied
// because the node is full, and creates CAST AI Container Live Migration
// (CLM) Migration CRDs to move them to nodes where the resize can succeed.
type Config struct {
	DryRun                  bool
	PendingThreshold        time.Duration
	SafetyScanInterval      time.Duration
	MigrationTimeout        time.Duration
	MigrationRetryLimit     int
	MigrationRetryDelay     time.Duration
	MigrationAlertThreshold int // migrations per workload per hour
	CLMNodeTemplate         string
	LeaderElection          bool
	LeaseName               string
	PodNamespace            string
	PodName                 string
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		DryRun:                  getBool("DRY_RUN", true),
		PendingThreshold:        getDuration("PENDING_THRESHOLD", 2*time.Minute),
		SafetyScanInterval:      getDuration("SAFETY_SCAN_INTERVAL", 1*time.Minute),
		MigrationTimeout:        getDuration("MIGRATION_TIMEOUT", 10*time.Minute),
		MigrationRetryLimit:     getInt("MIGRATION_RETRY_LIMIT", 3),
		MigrationRetryDelay:     getDuration("MIGRATION_RETRY_DELAY", 30*time.Second),
		MigrationAlertThreshold: getInt("MIGRATION_ALERT_THRESHOLD", 3),
		CLMNodeTemplate:         getString("CLM_NODE_TEMPLATE", "clm-live-migration-template"),
		LeaderElection:          getBool("LEADER_ELECTION_ENABLED", true),
		LeaseName:               getString("LEADER_ELECTION_LEASE_NAME", "castai-workload-resize-migrator"),
		PodNamespace:            getString("POD_NAMESPACE", ""),
		PodName:                 getString("POD_NAME", ""),
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
		return fallback
	}
	return i
}
