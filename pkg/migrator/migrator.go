package migrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"woop-rebalance-controller/pkg/config"
	"woop-rebalance-controller/pkg/detector"

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

// Client creates and tracks CAST AI CLM Migration CRDs.
type Client struct {
	cfg    config.Config
	client dynamic.Interface

	mu             sync.Mutex
	activeMigrations map[string]time.Time // namespace/name -> creation time
}

// New creates a new migrator client.
func New(cfg config.Config, client dynamic.Interface) *Client {
	return &Client{
		cfg:              cfg,
		client:           client,
		activeMigrations: make(map[string]time.Time),
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
	c.cleanupActiveMigrations()
	if _, active := c.activeMigrations[key]; active {
		slog.Info("skipping pod already being migrated", "pod", key)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

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
					"woop-rebalance-controller/managed": "true",
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
	c.activeMigrations[key] = time.Now()
	c.mu.Unlock()

	return nil
}

// IsActive returns true if the controller has recently created a migration for the pod.
func (c *Client) IsActive(namespace, podName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupActiveMigrations()
	_, ok := c.activeMigrations[namespace+"/"+podName]
	return ok
}

func (c *Client) cleanupActiveMigrations() {
	now := time.Now()
	for k, t := range c.activeMigrations {
		if now.Sub(t) > c.cfg.MigrationTimeout {
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
