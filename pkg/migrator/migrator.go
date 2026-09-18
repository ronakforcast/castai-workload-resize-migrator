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

var (
	migrationGVR = schema.GroupVersionResource{
		Group:    "live.cast.ai",
		Version:  "v1",
		Resource: "migrations",
	}

	nodeGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "nodes",
	}
)

const (
	// managedLabelKey marks Migration CRDs created by this controller.
	// AdoptExisting uses it as the cluster-wide selector when rebuilding
	// in-memory tracking after a restart or leader failover.
	managedLabelKey   = "castai-workload-resize-migrator/managed"
	managedLabelValue = "true"
)

// migrationEntry tracks a single migration's lifecycle.
type migrationEntry struct {
	name       string
	namespace  string
	createdAt  time.Time
	migration  string // migration CRD name
	retryCount int
}

// failedDestination records a destination node that exhausted the retry
// limit for a pod, together with when the exclusion expires.
type failedDestination struct {
	node  string
	until time.Time
}

// Client creates and tracks CAST AI CLM Migration CRDs.
type Client struct {
	cfg    config.Config
	client dynamic.Interface

	mu               sync.Mutex
	activeMigrations map[string]*migrationEntry // pod key -> entry
	nameCounter      atomic.Uint64              // monotonic counter for unique migration names

	// podMigrations tracks migration-creation timestamps per pod key for
	// the per-hour rate limit (circuit breaker). Entries older than an
	// hour are pruned on access.
	podMigrations map[string][]time.Time

	// failedDestinations maps pod key -> destination node that exhausted
	// the retry limit, with an expiry. selectDestinationNode skips it for
	// that pod until the TTL lapses, then falls back to any candidate
	// rather than stranding the pod.
	failedDestinations map[string]failedDestination
}

// New creates a new migrator client.
func New(cfg config.Config, client dynamic.Interface) *Client {
	return &Client{
		cfg:                cfg,
		client:             client,
		activeMigrations:   make(map[string]*migrationEntry),
		podMigrations:      make(map[string][]time.Time),
		failedDestinations: make(map[string]failedDestination),
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
	} else {
		// Reserve the key up front so a concurrent trigger for the same pod
		// (informer worker racing the safety scan) cannot also create a
		// Migration CRD. createMigration fills in the reserved entry.
		entry = &migrationEntry{
			name:      p.PodName,
			namespace: p.Namespace,
			createdAt: time.Now(),
		}
		c.activeMigrations[key] = entry
	}
	c.mu.Unlock()

	if active {
		return c.handleExistingMigration(ctx, p, key, entry, snapshot)
	}

	if err := c.createMigration(ctx, p, 0); err != nil {
		// Release the reservation if it is still ours and no migration was
		// recorded, so a later attempt can retry cleanly.
		c.mu.Lock()
		if cur, ok := c.activeMigrations[key]; ok && cur == entry && cur.migration == "" {
			delete(c.activeMigrations, key)
		}
		c.mu.Unlock()
		return err
	}
	if c.cfg.DryRun {
		// Dry-run created no Migration CRD: release the reservation so the
		// pod is not considered actively migrating.
		c.mu.Lock()
		if cur, ok := c.activeMigrations[key]; ok && cur == entry {
			delete(c.activeMigrations, key)
		}
		c.mu.Unlock()
	}
	return nil
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

// selectDestinationNode picks a destination node for a migration of a pod
// running on sourceNode. A node qualifies as a destination only if all of
// these hold:
//
//  1. it is not the source node — the live migration controller rejects
//     same-node migrations;
//  2. it carries the live.cast.ai/migration-enabled=true label;
//  3. it was provisioned from the configured CLM node template (nodes
//     carry the template name in the scheduling.cast.ai/node-template
//     label). The template's required single-subnet/single-AZ and
//     single-CPU-generation setup is what guarantees live-migration
//     compatibility, so no zone or capability checks are needed here;
//  4. check 3 is skipped when CLMNodeTemplate is empty (no scoping).
//
// The first qualifying node is returned. If no candidate exists, an error
// is returned so the caller fails safely without creating a migration.
func (c *Client) selectDestinationNode(ctx context.Context, sourceNode, podKey string) (string, error) {
	list, err := c.client.Resource(nodeGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}

	// A destination that exhausted this pod's retry limit is skipped for
	// FailedDestinationTTL; after the TTL it becomes eligible again.
	excluded := ""
	if fd, ok := c.failedDestinations[podKey]; ok {
		if time.Now().Before(fd.until) {
			excluded = fd.node
		} else {
			delete(c.failedDestinations, podKey)
		}
	}

	candidate := ""
	fallback := ""
	for _, n := range list.Items {
		name := n.GetName()
		if name == sourceNode {
			continue
		}
		labels, _, _ := unstructured.NestedStringMap(n.Object, "metadata", "labels")
		if labels == nil || labels["live.cast.ai/migration-enabled"] != "true" {
			continue
		}
		// Only nodes from the configured CLM node template are valid
		// destinations (instance-family compatibility for live migration).
		if tmpl := labels["scheduling.cast.ai/node-template"]; c.cfg.CLMNodeTemplate != "" && tmpl != c.cfg.CLMNodeTemplate {
			continue
		}
		// DestinationNodeSelector: every configured label pair must match
		// (e.g. pin topology.kubernetes.io/zone for zonal PVCs whose data
		// cannot cross AZs). Empty selector disables the filter.
		if !nodeMatchesSelector(labels, c.cfg.DestinationNodeSelector) {
			continue
		}
		if name == excluded {
			// Remember it only as a last-resort fallback: if it is the
			// sole candidate, re-selecting it still beats stranding the pod.
			fallback = name
			continue
		}
		candidate = name
		break
	}

	if candidate == "" && fallback != "" {
		slog.Warn("only the previously failed destination remains; re-selecting it to avoid stranding the pod",
			"pod", podKey, "node", fallback)
		candidate = fallback
	}

	if candidate != "" {
		return candidate, nil
	}
	return "", fmt.Errorf("no live-migration-enabled node from node template %q available as destination (source node %q excluded); failing safely without creating migration",
		c.cfg.CLMNodeTemplate, sourceNode)
}

// nodeMatchesSelector reports whether a node's labels satisfy every
// key=value pair in the selector. A nil/empty selector matches any node.
func nodeMatchesSelector(labels map[string]string, selector map[string]string) bool {
	for k, want := range selector {
		if labels == nil || labels[k] != want {
			return false
		}
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
		managedLabelKey: managedLabelValue,
	}
	if c.cfg.CLMNodeTemplate != "" {
		labels["castai-workload-resize-migrator/clm-node-template"] = c.cfg.CLMNodeTemplate
	}

	// Circuit breaker: cap migrations per pod per hour so a permanently
	// failing pod cannot loop create -> fail -> re-trigger forever.
	if c.cfg.MigrationRateLimitPerHour > 0 {
		now := time.Now()
		c.mu.Lock()
		history := c.podMigrations[key]
		kept := history[:0]
		for _, t := range history {
			if now.Sub(t) < time.Hour {
				kept = append(kept, t)
			}
		}
		if len(kept) >= c.cfg.MigrationRateLimitPerHour {
			c.mu.Unlock()
			slog.Warn("migration rate limit reached for pod; skipping (circuit breaker)",
				"pod", key,
				"createdLastHour", len(kept),
				"limit", c.cfg.MigrationRateLimitPerHour,
			)
			return fmt.Errorf("pod %s exceeded migration rate limit (%d/hour)", key, c.cfg.MigrationRateLimitPerHour)
		}
		c.podMigrations[key] = append(kept, now)
		c.mu.Unlock()
	}

	// Concurrency cap: a mass-resize event must not stampede the cluster
	// with migrations all at once. Retries of already-tracked pods are
	// exempt so the cap cannot starve recovery of in-flight entries.
	if c.cfg.MaxConcurrentMigrations > 0 && retryCount == 0 {
		c.mu.Lock()
		active := len(c.activeMigrations)
		c.mu.Unlock()
		if active >= c.cfg.MaxConcurrentMigrations {
			slog.Warn("max concurrent migrations reached; deferring new migration to next scan",
				"pod", key, "active", active, "max", c.cfg.MaxConcurrentMigrations)
			return fmt.Errorf("max concurrent migrations reached (%d)", c.cfg.MaxConcurrentMigrations)
		}
	}

	// Destination must be a live-migration-enabled node from the same CLM
	// node template as the pod's current node, and must differ from it: the
	// live migration controller rejects same-node migrations. A
	// destination that previously exhausted this pod's retry limit is
	// skipped until FailedDestinationTTL lapses. If no candidate exists,
	// the migration fails safely and nothing is created.
	destination, err := c.selectDestinationNode(ctx, p.NodeName, key)
	if err != nil {
		// The rate-limit slot reserved above must not be consumed by a
		// failed destination selection.
		if c.cfg.MigrationRateLimitPerHour > 0 {
			c.mu.Lock()
			if h := c.podMigrations[key]; len(h) > 0 {
				c.podMigrations[key] = h[:len(h)-1]
			}
			c.mu.Unlock()
		}
		return fmt.Errorf("select destination node for %s: %w", key, err)
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
				"destination": destination,
			},
		},
	}

	_, err = c.client.Resource(migrationGVR).Namespace(p.Namespace).Create(ctx, migration, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create migration %s/%s: %w", p.Namespace, migrationName, err)
	}

	slog.Info("migration created", "pod", key, "migration", migrationName, "node", p.NodeName, "destination", destination)

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

// getMigrationDestination fetches the spec.destination of a Migration CRD.
func (c *Client) getMigrationDestination(ctx context.Context, namespace, name string) (string, error) {
	obj, err := c.client.Resource(migrationGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	dest, found, err := unstructured.NestedString(obj.Object, "spec", "destination")
	if err != nil || !found {
		return "", fmt.Errorf("spec.destination not found: %w", err)
	}
	return dest, nil
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
				// Remember the destination so the pod's next cycle migrates
				// elsewhere (H3 companion: same node for the in-cycle retries,
				// then move on). The exclusion lapses after
				// FailedDestinationTTL.
				if c.cfg.FailedDestinationTTL > 0 {
					if dest, err := c.getMigrationDestination(ctx, it.namespace, it.migration); err == nil && dest != "" {
						c.mu.Lock()
						c.failedDestinations[it.key] = failedDestination{node: dest, until: time.Now().Add(c.cfg.FailedDestinationTTL)}
						c.mu.Unlock()
						slog.Warn("destination exhausted the retry limit; excluding it for this pod until TTL", "pod", it.key, "node", dest, "ttl", c.cfg.FailedDestinationTTL.String())
					}
				}
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

// AdoptExisting rebuilds the in-memory migration tracking from the
// Migration CRDs this controller previously created (labeled
// castai-workload-resize-migrator/managed=true). It exists for restarts
// and leader failovers: without it the new leader starts with an empty
// map and would create a duplicate Migration CRD for every pod whose
// migration is still in flight.
//
// Semantics per CRD:
//   - Completed/Succeeded: ignored (nothing left to track).
//   - Failed: adopted with retryCount pinned at MigrationRetryLimit so a
//     failover cannot mint a fresh retry budget; the cleanup loop drops
//     the entry on its next tick (Failed + limit reached is its existing
//     removal condition), after which a fresh trigger cycle can begin.
//   - Any other state (Pending, Running, empty, unknown): adopted with
//     retryCount 0.
//
// createdAt is seeded from the CRD's creationTimestamp so
// MIGRATION_TIMEOUT keeps working across restarts.
//
// Call once per leadership term (runController invokes it right after
// informer cache sync). Safe to call concurrently with Trigger: an entry
// already reserved by an in-flight trigger is never overwritten, and
// repeated calls are idempotent by pod key. Returns the number of newly
// adopted migrations.
func (c *Client) AdoptExisting(ctx context.Context) (int, error) {
	list, err := c.client.Resource(migrationGVR).List(ctx, metav1.ListOptions{
		LabelSelector: managedLabelKey + "=" + managedLabelValue,
	})
	if err != nil {
		return 0, fmt.Errorf("list managed migrations for adoption: %w", err)
	}

	adopted := 0
	for i := range list.Items {
		item := &list.Items[i]

		podName, found, err := unstructured.NestedString(item.Object, "spec", "podName")
		if err != nil || !found || podName == "" {
			slog.Warn("managed migration missing spec.podName; skipping adoption",
				"migration", item.GetName(),
				"namespace", item.GetNamespace(),
			)
			continue
		}

		state, _, _ := unstructured.NestedString(item.Object, "status", "state")
		switch state {
		case "Completed", "Succeeded":
			// Terminal success: nothing left to track.
			continue
		}

		entry := &migrationEntry{
			name:      podName,
			namespace: item.GetNamespace(),
			createdAt: item.GetCreationTimestamp().Time,
			migration: item.GetName(),
		}
		if state == "Failed" {
			// Pin the retry budget: a failover must not mint a fresh one.
			entry.retryCount = c.cfg.MigrationRetryLimit
		}

		key := item.GetNamespace() + "/" + podName
		c.mu.Lock()
		if _, exists := c.activeMigrations[key]; !exists {
			c.activeMigrations[key] = entry
			adopted++
		}
		c.mu.Unlock()
	}

	return adopted, nil
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
