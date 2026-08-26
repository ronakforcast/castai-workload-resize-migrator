package migrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

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

	mu              sync.Mutex
	activeMigrations map[string]*migrationEntry // pod key -> entry
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
func (c *Client) Trigger(ctx context.Context, pods []*detector.PodPendingInfo) error {
	for _, p := range pods {
		if err := c.triggerOne(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) triggerOne(ctx context.Context, p *detector.PodPendingInfo) error {
	key := fmt.Sprintf("%s/%s", p.Namespace, p.PodName)

	c.mu.Lock()
	c.cleanupExpiredMigrations()
	if entry, active := c.activeMigrations[key]; active {
		// Check if the existing migration has failed and can be retried.
		if c.shouldRetry(ctx, key, entry) {
			slog.Info("retrying failed migration", "pod", key, "migration", entry.migration, "retry", entry.retryCount+1)
			entry.retryCount++
			entry.createdAt = time.Now()
			c.mu.Unlock()
			return c.createMigration(ctx, p)
		}
		slog.Debug("skipping pod already being migrated", "pod", key, "migration", entry.migration)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	return c.createMigration(ctx, p)
}

func (c *Client) createMigration(ctx context.Context, p *detector.PodPendingInfo) error {
	key := fmt.Sprintf("%s/%s", p.Namespace, p.PodName)
	migrationName := generateMigrationName(p)

	if c.cfg.DryRun {
		slog.Info("DRY-RUN: would create migration", "pod", key, "migration", migrationName, "node", p.NodeName, "desired", p.DesiredCPU, "allocated", p.AllocatedCPU)
		return nil
	}

	migration := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "live.cast.ai/v1",
			"kind":       "Migration",
			"metadata": map[string]interface{}{
				"name":      migrationName,
				"namespace": p.Namespace,
				"labels": map[string]interface{}{
					"castai-workload-resize-migrator/managed": "true",
				},
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
	c.activeMigrations[key] = &migrationEntry{
		name:       p.PodName,
		namespace:  p.Namespace,
		createdAt:  time.Now(),
		migration:  migrationName,
		retryCount: 0,
	}
	c.mu.Unlock()

	return nil
}

// shouldRetry checks if a migration has failed and can be retried.
func (c *Client) shouldRetry(ctx context.Context, key string, entry *migrationEntry) bool {
	// Don't retry if we've hit the retry limit.
	if entry.retryCount >= c.cfg.MigrationRetryLimit {
		return false
	}

	// Don't retry if the migration was created too recently (avoid hammering).
	if time.Since(entry.createdAt) < c.cfg.MigrationRetryDelay {
		return false
	}

	// Check the migration status.
	state, err := c.getMigrationState(ctx, entry.namespace, entry.migration)
	if err != nil {
		slog.Debug("failed to get migration state, not retrying", "pod", key, "error", err)
		return false
	}

	if state == "Failed" {
		slog.Info("migration failed, will retry", "pod", key, "migration", entry.migration, "state", state, "retryCount", entry.retryCount)
		return true
	}

	return false
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

// CleanupCompletedMigrations removes entries for migrations that have completed or failed
// beyond the retry limit. This should be called periodically.
func (c *Client) CleanupCompletedMigrations(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, entry := range c.activeMigrations {
		// Expire entries that are older than the migration timeout.
		if time.Since(entry.createdAt) > c.cfg.MigrationTimeout {
			state, err := c.getMigrationState(ctx, entry.namespace, entry.migration)
			if err != nil {
				slog.Debug("failed to get migration state during cleanup", "pod", key, "error", err)
				continue
			}

			if state == "Completed" {
				slog.Info("migration completed, removing from tracking", "pod", key, "migration", entry.migration)
				delete(c.activeMigrations, key)
			} else if state == "Failed" && entry.retryCount >= c.cfg.MigrationRetryLimit {
				slog.Info("migration failed and retry limit reached, removing from tracking", "pod", key, "migration", entry.migration, "retries", entry.retryCount)
				delete(c.activeMigrations, key)
			}
		}
	}
}

// IsActive returns true if the controller has an active migration for the pod.
func (c *Client) IsActive(namespace, podName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupExpiredMigrations()
	_, ok := c.activeMigrations[namespace+"/"+podName]
	return ok
}

func (c *Client) cleanupExpiredMigrations() {
	now := time.Now()
	for k, entry := range c.activeMigrations {
		if now.Sub(entry.createdAt) > c.cfg.MigrationTimeout {
			delete(c.activeMigrations, k)
		}
	}
}

func generateMigrationName(p *detector.PodPendingInfo) string {
	base := p.PodName
	if len(base) > 50 {
		base = base[:50]
	}
	return fmt.Sprintf("%s-woop-%d", base, time.Now().Unix())
}
