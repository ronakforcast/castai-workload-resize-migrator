package migrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var migrationGVR = schema.GroupVersionResource{
	Group:    "live.cast.ai",
	Version:  "v1",
	Resource: "migrations",
}

// migrationEntry tracks a single migration's lifecycle.
type migrationEntry struct {
	name       string
	namespace  string
	createdAt  time.Time
	migration  string // migration CRD name
	retryCount int
}

// Client creates and tracks CAST AI CLM Migration CRDs.
type Client struct {
	cfg    config.Config
	client dynamic.Interface

	mu               sync.Mutex
	activeMigrations map[string]*migrationEntry // pod key -> entry
	nameCounter      atomic.Uint64              // monotonic counter for unique migration names
}

// New creates a new migrator client.
func New(cfg config.Config, client dynamic.Interface) *Client {
	return &Client{
		cfg:              cfg,
		client:           client,
		activeMigrations: make(map[string]*migrationEntry),
	}
}

// Trigger creates a Migration CRD for each suspect pod that is not already being migrated.
// All Kubernetes API calls triggered by this method happen outside c.mu.
//
// A failure on one pod does not abort the batch: each failing pod is
// logged and skipped so a single bad pod cannot starve the rest of a
// safety-scan batch. The returned error is the join of all individual
// failures (nil when every pod succeeded).
func (c *Client) Trigger(ctx context.Context, pods []*detector.PodPendingInfo) error {
	var errs []error
	for _, p := range pods {
		if err := c.triggerOne(ctx, p); err != nil {
			slog.Error("migration trigger failed for pod",
				"pod", p.Namespace+"/"+p.PodName,
				"error", err,
			)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// triggerOne handles migration creation for a single pod. All Kubernetes API
// calls happen outside c.mu so they never block other triggers.
func (c *Client) triggerOne(ctx context.Context, p *detector.PodPendingInfo) error {
	key := fmt.Sprintf("%s/%s", p.Namespace, p.PodName)

	// Snapshot the entry under the lock, then release it before any API call.
	// Copy the entry by value so subsequent reads outside the lock are race-free.
	c.mu.Lock()
	entry, active := c.activeMigrations[key]
	var snapshot migrationEntry
	if active {
		snapshot = *entry
	}
	c.mu.Unlock()

	if active {
		return c.handleExistingMigration(ctx, p, key, entry, snapshot)
	}

	return c.createMigration(ctx, p, 0)
}

// handleExistingMigration decides whether to retry a tracked pod's migration.
// All Kubernetes API calls happen outside c.mu. The snapshot is a copy of the
// entry taken under the lock and is safe to read without locking.
func (c *Client) handleExistingMigration(ctx context.Context, p *detector.PodPendingInfo, key string, entry *migrationEntry, snapshot migrationEntry) error {
	// Cheap local checks first.
	if !c.retryPolicyAllows(&snapshot) {
		slog.Debug("skipping pod: retry policy not satisfied", "pod", key, "migration", snapshot.migration, "retryCount", snapshot.retryCount)
		return nil
	}

	// Fetch the migration status outside the lock.
	state, err := c.getMigrationState(ctx, snapshot.namespace, snapshot.migration)
	if err != nil {
		slog.Debug("failed to get migration state, not retrying", "pod", key, "error", err)
		return nil
	}

	if state != "Failed" {
		slog.Debug("migration not in Failed state, skipping", "pod", key, "migration", snapshot.migration, "state", state)
		return nil
	}

	// Atomically claim the retry slot. Re-verify the entry is still the same
	// pointer to handle concurrent cleanup or trigger racing, and re-check the
	// retry policy in case it changed since the snapshot.
	c.mu.Lock()
	cur, ok := c.activeMigrations[key]
	if !ok || cur != entry || !c.retryPolicyAllows(cur) {
		c.mu.Unlock()
		return nil
	}
	cur.retryCount++
	cur.createdAt = time.Now()
	retryCount := cur.retryCount
	c.mu.Unlock()

	slog.Info("retrying failed migration", "pod", key, "migration", snapshot.migration, "retry", retryCount)
	return c.createMigration(ctx, p, retryCount)
}

// retryPolicyAllows reports whether the retry policy permits another attempt
// based on local-only checks (retry limit, retry delay). It does not perform
// any Kubernetes API calls.
func (c *Client) retryPolicyAllows(entry *migrationEntry) bool {
	if entry.retryCount >= c.cfg.MigrationRetryLimit {
		return false
	}
	if time.Since(entry.createdAt) < c.cfg.MigrationRetryDelay {
		return false
	}
	return true
}

// createMigration creates a new Migration CRD for the pod and records the
// resulting entry in activeMigrations. The supplied retryCount is preserved
// when an existing entry is updated; otherwise it is used for new entries.
func (c *Client) createMigration(ctx context.Context, p *detector.PodPendingInfo, retryCount int) error {
	key := fmt.Sprintf("%s/%s", p.Namespace, p.PodName)
	migrationName := c.generateMigrationName(p)

	if c.cfg.DryRun {
		slog.Info("DRY-RUN: would create migration", "pod", key, "migration", migrationName, "node", p.NodeName, "desired", p.DesiredCPU, "allocated", p.AllocatedCPU)
		return nil
	}

	labels := map[string]interface{}{
		"castai-workload-resize-migrator/managed": "true",
	}
	if c.cfg.CLMNodeTemplate != "" {
		labels["castai-workload-resize-migrator/clm-node-template"] = c.cfg.CLMNodeTemplate
	}

	migration := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "live.cast.ai/v1",
			"kind":       "Migration",
			"metadata": map[string]interface{}{
				"name":      migrationName,
				"namespace": p.Namespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"podName":     p.PodName,
				"destination": p.NodeName,
			},
		},
	}

	_, err := c.client.Resource(migrationGVR).Namespace(p.Namespace).Create(ctx, migration, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create migration %s/%s: %w", p.Namespace, migrationName, err)
	}

	slog.Info("migration created", "pod", key, "migration", migrationName, "node", p.NodeName)

	c.mu.Lock()
	if cur, ok := c.activeMigrations[key]; ok {
		// Preserve the incremented retry count and update the tracked migration name.
		cur.retryCount = retryCount
		cur.createdAt = time.Now()
		cur.migration = migrationName
	} else {
		c.activeMigrations[key] = &migrationEntry{
			name:       p.PodName,
			namespace:  p.Namespace,
			createdAt:  time.Now(),
			migration:  migrationName,
			retryCount: retryCount,
		}
	}
	c.mu.Unlock()

	return nil
}

// getMigrationState fetches the status.state of a Migration CRD.
func (c *Client) getMigrationState(ctx context.Context, namespace, name string) (string, error) {
	obj, err := c.client.Resource(migrationGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	state, found, err := unstructured.NestedString(obj.Object, "status", "state")
	if err != nil || !found {
		return "", fmt.Errorf("status.state not found: %w", err)
	}
	return state, nil
}

// CleanupCompletedMigrations removes tracked entries whose underlying Migration
// CRD has reached a terminal state. It performs all Kubernetes API calls
// outside c.mu and is safe to call concurrently with Trigger.
func (c *Client) CleanupCompletedMigrations(ctx context.Context) {
	c.cleanupExpiredMigrations(ctx)
}

// cleanupExpiredMigrations removes tracked entries based on the Migration CRD
// status rather than wall-clock createdAt. All Kubernetes API calls happen
// outside c.mu.
func (c *Client) cleanupExpiredMigrations(ctx context.Context) {
	type snapshotItem struct {
		key string
		// entry is kept solely for pointer-identity re-verification
		// before deletion; all mutable fields are copied below and
		// read without the lock.
		entry      *migrationEntry
		namespace  string
		migration  string
		retryCount int
		createdAt  time.Time
	}

	// Snapshot everything cleanup needs under the lock. Copying the
	// mutable fields by value (rather than reading them through the
	// entry pointer after unlocking) keeps this loop race-free against
	// concurrent handleExistingMigration/createMigration writes, which
	// mutate retryCount and createdAt under the same lock.
	c.mu.Lock()
	items := make([]snapshotItem, 0, len(c.activeMigrations))
	for k, e := range c.activeMigrations {
		items = append(items, snapshotItem{
			key:        k,
			entry:      e,
			namespace:  e.namespace,
			migration:  e.migration,
			retryCount: e.retryCount,
			createdAt:  e.createdAt,
		})
	}
	c.mu.Unlock()

	for _, it := range items {
		state, err := c.getMigrationState(ctx, it.namespace, it.migration)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// The Migration CR is gone (deleted manually or by an
				// external GC). Tracking it further is pointless — and
				// would permanently hide the pod from the detector
				// (IsActive stays true) — so drop the entry. The pod
				// becomes eligible again on the next informer event or
				// safety scan. Issue #1, item F2.
				slog.Info("migration CR not found; dropping tracking so the pod can be re-triggered",
					"pod", it.key,
					"migration", it.migration,
				)
				c.mu.Lock()
				if cur, ok := c.activeMigrations[it.key]; ok && cur == it.entry {
					delete(c.activeMigrations, it.key)
				}
				c.mu.Unlock()
				continue
			}
			slog.Debug("failed to get migration state during cleanup", "pod", it.key, "error", err)
			continue
		}

		remove := false
		switch state {
		case "Completed":
			slog.Info("migration completed, removing from tracking", "pod", it.key, "migration", it.migration)
			remove = true
		case "Succeeded":
			slog.Info("migration succeeded, removing from tracking", "pod", it.key, "migration", it.migration)
			remove = true
		case "Failed":
			if it.retryCount >= c.cfg.MigrationRetryLimit {
				slog.Info("migration failed and retry limit reached, removing from tracking", "pod", it.key, "migration", it.migration, "retries", it.retryCount)
				remove = true
			}
		default:
			// Non-terminal state (Running, Pending, etc.): apply wall-clock
			// timeout. A zero or negative timeout disables this check.
			if c.cfg.MigrationTimeout > 0 && time.Since(it.createdAt) > c.cfg.MigrationTimeout {
				slog.Warn("migration timed out in non-terminal state, removing from tracking",
					"pod", it.key,
					"migration", it.migration,
					"state", state,
					"age", time.Since(it.createdAt).String(),
					"timeout", c.cfg.MigrationTimeout.String(),
				)
				remove = true
			}
		}

		if !remove {
			continue
		}

		// Re-verify the entry is still the same pointer before deleting, to avoid
		// removing a re-created migration that happens to share a key.
		c.mu.Lock()
		if cur, ok := c.activeMigrations[it.key]; ok && cur == it.entry {
			delete(c.activeMigrations, it.key)
		}
		c.mu.Unlock()
	}
}

// IsActive returns true if the controller has an active migration for the pod.
// It performs no Kubernetes API calls.
func (c *Client) IsActive(namespace, podName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.activeMigrations[namespace+"/"+podName]
	return ok
}

// generateMigrationName produces a unique Migration CRD name. The atomic
// counter plus nanosecond timestamp guarantees uniqueness even under
// concurrent triggers for the same pod, while the base name is truncated
// to keep the total length under 64 characters.
//
// Format: <base>-woop-<nanoseconds>-<counter>
// Worst case length: 20 + 6 + 16 + 1 + 16 = 59 chars (well under 64).
func (c *Client) generateMigrationName(p *detector.PodPendingInfo) string {
	base := p.PodName
	if len(base) > 20 {
		base = base[:20]
	}
	n := c.nameCounter.Add(1)
	return fmt.Sprintf("%s-woop-%x-%x", base, time.Now().UnixNano(), n)
}
