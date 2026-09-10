package migrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func newFakeMigrator(t *testing.T, cfg config.Config) *Client {
	t.Helper()
	client := newFakeDynamicClient()
	return New(cfg, client)
}

func newFakeDynamicClient() *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		migrationGVR: "MigrationList",
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
}

// seedMigration puts a Migration CRD with the given state into the fake
// dynamic client so that getMigrationState can return it.
func seedMigration(t *testing.T, client dynamic.Interface, namespace, name, state string) {
	t.Helper()
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "live.cast.ai/v1",
			"kind":       "Migration",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"status": map[string]interface{}{
				"state": state,
			},
		},
	}
	if _, err := client.Resource(migrationGVR).Namespace(namespace).Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if _, uerr := client.Resource(migrationGVR).Namespace(namespace).Update(context.Background(), obj, metav1.UpdateOptions{}); uerr != nil {
				t.Fatalf("seed migration %s/%s: %v", namespace, name, uerr)
			}
			return
		}
		t.Fatalf("seed migration %s/%s: %v", namespace, name, err)
	}
}

func samplePod() *detector.PodPendingInfo {
	return &detector.PodPendingInfo{
		Namespace:    "default",
		PodName:      "nginx",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   400,
	}
}

func samplePods() []*detector.PodPendingInfo {
	return []*detector.PodPendingInfo{
		{Namespace: "default", PodName: "pod-a", NodeName: "node-1", AllocatedCPU: 100, DesiredCPU: 400},
		{Namespace: "default", PodName: "pod-b", NodeName: "node-2", AllocatedCPU: 100, DesiredCPU: 400},
		{Namespace: "default", PodName: "pod-c", NodeName: "node-1", AllocatedCPU: 100, DesiredCPU: 400},
	}
}

func TestTriggerDryRun(t *testing.T) {
	cfg := config.Config{DryRun: true}
	m := newFakeMigrator(t, cfg)

	if err := m.Trigger(context.Background(), []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.IsActive("default", "nginx") {
		t.Fatal("expected no active migration in dry-run")
	}
}

func TestTriggerCreatesMigration(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	if err := m.Trigger(context.Background(), []*detector.PodPendingInfo{samplePod()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !m.IsActive("default", "nginx") {
		t.Fatal("expected active migration after trigger")
	}
}

func TestTriggerSkipsActivePod(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	pods := []*detector.PodPendingInfo{samplePod()}
	if err := m.Trigger(context.Background(), pods); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Second trigger must not error and must not create a duplicate CRD.
	if err := m.Trigger(context.Background(), pods); err != nil {
		t.Fatalf("unexpected error on second trigger: %v", err)
	}
}

func TestTriggerEmptyPodList(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	if err := m.Trigger(context.Background(), []*detector.PodPendingInfo{}); err != nil {
		t.Fatalf("unexpected error on empty list: %v", err)
	}
}

func TestTriggerMultiplePods(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	if err := m.Trigger(context.Background(), samplePods()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range []string{"pod-a", "pod-b", "pod-c"} {
		if !m.IsActive("default", name) {
			t.Fatalf("expected %s to be active", name)
		}
	}
}

func TestIsActiveNonExistentPod(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	m := newFakeMigrator(t, cfg)

	if m.IsActive("default", "nonexistent") {
		t.Fatal("expected IsActive=false for non-existent pod")
	}
}

func TestGenerateMigrationNameIsUniqueAndShort(t *testing.T) {
	p := &detector.PodPendingInfo{PodName: "nginx"}
	m := New(config.Config{}, newFakeDynamicClient())

	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		name := m.generateMigrationName(p)
		if len(name) >= 64 {
			t.Fatalf("migration name too long: %d chars: %s", len(name), name)
		}
		if !strings.HasPrefix(name, "nginx-woop-") {
			t.Fatalf("unexpected prefix on name %q", name)
		}
		if _, dup := seen[name]; dup {
			t.Fatalf("duplicate migration name generated: %s", name)
		}
		seen[name] = struct{}{}
	}
}

func TestGenerateMigrationNameTruncatesLongPodName(t *testing.T) {
	longName := "this-is-a-very-long-pod-name-that-exceeds-twenty-chars"
	p := &detector.PodPendingInfo{PodName: longName}
	m := New(config.Config{}, newFakeDynamicClient())

	name := m.generateMigrationName(p)
	if len(name) >= 64 {
		t.Fatalf("migration name too long: %d chars: %s", len(name), name)
	}
	if !strings.HasPrefix(name, longName[:20]+"-woop-") {
		t.Fatalf("expected prefix %q in name %q", longName[:20], name)
	}
}

// TestRetryCountPreservedOnRetries ensures that when a migration fails and the
// trigger is retried, the retryCount is incremented on the existing entry
// rather than reset to 0.
func TestRetryCountPreservedOnRetries(t *testing.T) {
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 3,
		// Allow retries without delay so the test does not have to sleep.
		MigrationRetryDelay: 0,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)

	pod := samplePod()
	ctx := context.Background()

	// First trigger: creates the Migration CRD with no status yet.
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("first trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	firstName := m.activeMigrations[key].migration
	m.mu.Unlock()

	// Seed the API to report Failed for that migration.
	seedMigration(t, client, "default", firstName, "Failed")

	// Force the retry delay to have passed by rewinding createdAt.
	m.mu.Lock()
	m.activeMigrations[key].createdAt = time.Now().Add(-1 * time.Hour)
	m.mu.Unlock()

	// Second trigger: should retry, incrementing retryCount to 1.
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("retry trigger failed: %v", err)
	}
	m.mu.Lock()
	got := m.activeMigrations[key].retryCount
	m.mu.Unlock()
	if got != 1 {
		t.Fatalf("after first retry: want retryCount=1, got %d", got)
	}

	// Seed Failed again for the (possibly new) migration name and rewind time.
	m.mu.Lock()
	secondName := m.activeMigrations[key].migration
	m.activeMigrations[key].createdAt = time.Now().Add(-1 * time.Hour)
	m.mu.Unlock()
	seedMigration(t, client, "default", secondName, "Failed")

	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("second retry trigger failed: %v", err)
	}
	m.mu.Lock()
	got = m.activeMigrations[key].retryCount
	m.mu.Unlock()
	if got != 2 {
		t.Fatalf("after second retry: want retryCount=2, got %d", got)
	}
}

// TestRunningMigrationRemovedOnTimeout verifies that an entry whose CRD is
// still Running is removed when the entry is older than MigrationTimeout.
func TestRunningMigrationRemovedOnTimeout(t *testing.T) {
	cfg := config.Config{
		DryRun:           false,
		MigrationTimeout: 1 * time.Millisecond,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.activeMigrations[key].createdAt = time.Now().Add(-1 * time.Hour)
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Running")

	// Sleep so MigrationTimeout is clearly exceeded, then cleanup.
	time.Sleep(10 * time.Millisecond)
	m.CleanupCompletedMigrations(ctx)

	if m.IsActive("default", "nginx") {
		t.Fatal("expected Running migration to be removed from tracking after timeout")
	}
}

// TestPendingMigrationRemovedOnTimeout verifies that a Pending migration older
// than MigrationTimeout is also removed during cleanup (Pending is non-terminal).
func TestPendingMigrationRemovedOnTimeout(t *testing.T) {
	cfg := config.Config{
		DryRun:           false,
		MigrationTimeout: 1 * time.Millisecond,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.activeMigrations[key].createdAt = time.Now().Add(-1 * time.Hour)
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Pending")

	time.Sleep(10 * time.Millisecond)
	m.CleanupCompletedMigrations(ctx)

	if m.IsActive("default", "nginx") {
		t.Fatal("expected Pending migration to be removed from tracking after timeout")
	}
}

// TestRunningMigrationWithinTimeoutKept verifies that a Running migration
// within the MigrationTimeout window is not removed.
func TestRunningMigrationWithinTimeoutKept(t *testing.T) {
	cfg := config.Config{
		DryRun:           false,
		MigrationTimeout: 1 * time.Hour,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Running")

	m.CleanupCompletedMigrations(ctx)

	if !m.IsActive("default", "nginx") {
		t.Fatal("expected Running migration within timeout to remain tracked")
	}
}

// TestMigrationCarriesCLMNodeTemplateLabel verifies that a non-empty
// CLMNodeTemplate config value is propagated as a label on the created
// Migration CRD.
func TestMigrationCarriesCLMNodeTemplateLabel(t *testing.T) {
	const tmpl = "clm-live-migration-template"
	cfg := config.Config{
		DryRun:          false,
		CLMNodeTemplate: tmpl,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}

	m.mu.Lock()
	name := m.activeMigrations["default/nginx"].migration
	m.mu.Unlock()

	got, err := client.Resource(migrationGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get migration %s/%s: %v", "default", name, err)
	}
	val, found, err := unstructured.NestedString(got.Object, "metadata", "labels", "castai-workload-resize-migrator/clm-node-template")
	if err != nil {
		t.Fatalf("read label: %v", err)
	}
	if !found {
		t.Fatalf("expected clm-node-template label on migration %s", name)
	}
	if val != tmpl {
		t.Fatalf("clm-node-template label: want %q, got %q", tmpl, val)
	}
}

// TestMigrationOmitsCLMNodeTemplateLabelWhenEmpty verifies that the
// clm-node-template label is not present when the config value is empty.
func TestMigrationOmitsCLMNodeTemplateLabelWhenEmpty(t *testing.T) {
	cfg := config.Config{
		DryRun:          false,
		CLMNodeTemplate: "",
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}

	m.mu.Lock()
	name := m.activeMigrations["default/nginx"].migration
	m.mu.Unlock()

	got, err := client.Resource(migrationGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get migration %s/%s: %v", "default", name, err)
	}
	labels, _, err := unstructured.NestedMap(got.Object, "metadata", "labels")
	if err != nil {
		t.Fatalf("read labels: %v", err)
	}
	if _, present := labels["castai-workload-resize-migrator/clm-node-template"]; present {
		t.Fatalf("did not expect clm-node-template label when CLMNodeTemplate is empty; labels=%v", labels)
	}
	// The managed label must still be present.
	if labels["castai-workload-resize-migrator/managed"] != "true" {
		t.Fatalf("expected managed label; labels=%v", labels)
	}
}

// TestCompletedMigrationRemoved ensures a Completed migration is cleaned up.
func TestCompletedMigrationRemoved(t *testing.T) {
	cfg := config.Config{
		DryRun:           false,
		MigrationTimeout: 1 * time.Millisecond,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Completed")

	m.CleanupCompletedMigrations(ctx)

	if m.IsActive("default", "nginx") {
		t.Fatal("expected Completed migration to be cleaned up")
	}
}

// TestFailedMigrationAtRetryLimitRemoved ensures a Failed migration is cleaned
// up only when the retry limit has been reached.
func TestFailedMigrationAtRetryLimitRemoved(t *testing.T) {
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 2,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.activeMigrations[key].retryCount = 2
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Failed")

	m.CleanupCompletedMigrations(ctx)

	if m.IsActive("default", "nginx") {
		t.Fatal("expected Failed migration at retry limit to be cleaned up")
	}
}

// TestFailedMigrationBelowRetryLimitKept ensures a Failed migration that has
// not yet hit the retry limit stays tracked.
func TestFailedMigrationBelowRetryLimitKept(t *testing.T) {
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 3,
	}
	client := newFakeDynamicClient()
	m := New(cfg, client)
	ctx := context.Background()

	pod := samplePod()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.activeMigrations[key].retryCount = 1
	m.mu.Unlock()
	seedMigration(t, client, "default", name, "Failed")

	m.CleanupCompletedMigrations(ctx)

	if !m.IsActive("default", "nginx") {
		t.Fatal("expected Failed migration below retry limit to remain tracked")
	}
}

// TestAPICallDoesNotHoldMutex ensures that getMigrationState is called outside
// m.mu. The fake client below blocks Get until released; while it is blocked,
// IsActive and a concurrent Trigger for a different pod must be able to make
// progress.
func TestAPICallDoesNotHoldMutex(t *testing.T) {
	release := make(chan struct{})
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		migrationGVR: "MigrationList",
	}
	gate := &gatingClient{
		FakeDynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds),
		release:           release,
	}
	cfg := config.Config{
		DryRun:              false,
		MigrationTimeout:    10 * time.Minute,
		MigrationRetryLimit: 3,
		MigrationRetryDelay: 0,
	}
	m := New(cfg, gate)

	pod := samplePod()
	ctx := context.Background()
	if err := m.Trigger(ctx, []*detector.PodPendingInfo{pod}); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	key := "default/nginx"
	m.mu.Lock()
	name := m.activeMigrations[key].migration
	m.activeMigrations[key].createdAt = time.Now().Add(-1 * time.Hour)
	m.mu.Unlock()
	seedMigration(t, gate.FakeDynamicClient, "default", name, "Failed")

	// Kick off a retry in a goroutine; it will block inside the Get() call.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Trigger(ctx, []*detector.PodPendingInfo{pod})
	}()

	// Wait until the goroutine is parked inside the Get.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if atomic.LoadInt64(&gate.inFlight) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry goroutine never reached the API Get call")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// While the API Get is in flight, IsActive must be responsive. If m.mu
	// were held during the API call, this would deadlock against another
	// goroutine taking the same lock, so we verify it completes promptly.
	isActiveDone := make(chan bool, 1)
	go func() {
		isActiveDone <- m.IsActive("default", "nginx")
	}()
	select {
	case got := <-isActiveDone:
		if !got {
			t.Fatal("expected IsActive=true while API call in flight")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("IsActive blocked while API call in flight — mutex is held across API call")
	}

	// Now release the gated Get so the retry can finish.
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retry goroutine did not complete")
	}
}

// TestConcurrentTriggersNoDuplicateNames ensures that rapid concurrent triggers
// for the same pod do not produce duplicate migration names (no AlreadyExists
// from the API).
func TestConcurrentTriggersNoDuplicateNames(t *testing.T) {
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}
	client := newFakeDynamicClient()
	m := New(cfg, client)

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			err := m.Trigger(context.Background(), []*detector.PodPendingInfo{samplePod()})
			if err != nil {
				// Allow AlreadyExists only if it's a benign retry collision from
				// status-state races; the spec requires no duplicate-name errors.
				if apierrors.IsAlreadyExists(err) {
					errs <- errors.New("got AlreadyExists on concurrent trigger")
					return
				}
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent trigger error: %v", err)
	}

	m.mu.Lock()
	entry := m.activeMigrations["default/nginx"]
	m.mu.Unlock()
	if entry == nil {
		t.Fatal("expected exactly one active migration entry for the pod")
	}
}

// TestConcurrentTriggersNoDuplicateNamesRace runs the concurrent trigger test
// under the race detector to validate no data races in the lifecycle paths.
func TestConcurrentTriggersNoDuplicateNamesRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		TestConcurrentTriggersNoDuplicateNames(t)
	}
}

// --- helpers ---

// gatingClient is a dynamic.Interface wrapper that blocks the first Get call
// until release is closed. Used to verify that the migrator does not hold its
// mutex across the Kubernetes API call.
type gatingClient struct {
	*dynamicfake.FakeDynamicClient
	release  chan struct{}
	inFlight int64 // accessed via atomic ops
}

func (g *gatingClient) Resource(r schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	inner := g.FakeDynamicClient.Resource(r)
	return &gatingNamespaceable{NamespaceableResourceInterface: inner, release: g.release, counter: &g.inFlight}
}

type gatingNamespaceable struct {
	dynamic.NamespaceableResourceInterface // embedded; promotes all ResourceInterface methods
	release                                chan struct{}
	counter                                *int64
}

func (g *gatingNamespaceable) Namespace(ns string) dynamic.ResourceInterface {
	inner := g.NamespaceableResourceInterface.Namespace(ns)
	return &gatingResource{inner: inner, release: g.release, counter: g.counter}
}

type gatingResource struct {
	inner   dynamic.ResourceInterface
	release chan struct{}
	counter *int64
}

func (g *gatingResource) Get(ctx context.Context, name string, opts metav1.GetOptions, sub ...string) (*unstructured.Unstructured, error) {
	atomic.AddInt64(g.counter, 1)
	<-g.release
	defer atomic.AddInt64(g.counter, -1)
	return g.inner.Get(ctx, name, opts, sub...)
}

func (g *gatingResource) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, sub ...string) (*unstructured.Unstructured, error) {
	return g.inner.Create(ctx, obj, opts, sub...)
}

func (g *gatingResource) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, sub ...string) (*unstructured.Unstructured, error) {
	return g.inner.Update(ctx, obj, opts, sub...)
}

func (g *gatingResource) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	return g.inner.UpdateStatus(ctx, obj, opts)
}

func (g *gatingResource) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, sub ...string) error {
	return g.inner.Delete(ctx, name, opts, sub...)
}

func (g *gatingResource) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return g.inner.DeleteCollection(ctx, opts, listOpts)
}

func (g *gatingResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return g.inner.List(ctx, opts)
}

func (g *gatingResource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return g.inner.Watch(ctx, opts)
}

func (g *gatingResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, sub ...string) (*unstructured.Unstructured, error) {
	return g.inner.Patch(ctx, name, pt, data, opts, sub...)
}

func (g *gatingResource) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, sub ...string) (*unstructured.Unstructured, error) {
	return g.inner.Apply(ctx, name, obj, opts, sub...)
}

func (g *gatingResource) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return g.inner.ApplyStatus(ctx, name, obj, opts)
}

// TestCleanupConcurrentWithTriggerNoRace is the regression test for the
// data race in CleanupCompletedMigrations: the cleanup loop used to read
// entry.retryCount and entry.createdAt through the snapshot's entry
// pointer without holding c.mu, while handleExistingMigration writes
// those fields under the lock. Cleanup now copies the mutable fields
// into the snapshot under the lock; this test runs both paths
// concurrently so the race detector keeps it that way.
func TestCleanupConcurrentWithTriggerNoRace(t *testing.T) {
	client := newFakeDynamicClient()

	// Seed the Migration CRs BEFORE installing the create-failure
	// reactor (the reactor would make seeding fail too).
	seedMigration(t, client, "default", "mig-failed", "Failed")
	seedMigration(t, client, "default", "mig-running", "Running")

	// Make every create fail so handleExistingMigration's retry loop
	// keeps writing retryCount/createdAt on the SAME tracked entry: a
	// successful create would swap entry.migration to a fresh CR name
	// (which has no status), after which the retry path would stop
	// writing and the race would no longer be exercised.
	client.PrependReactor("create", "migrations", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("injected create failure")
	})

	cfg := config.Config{
		DryRun:              false,
		MigrationRetryLimit: math.MaxInt,
		MigrationRetryDelay: 0,
		MigrationTimeout:    time.Hour, // keep entries in the non-terminal timeout branch
	}
	c := New(cfg, client)

	// Seed the tracked entries directly (white-box) so each entry points
	// at exactly the seeded Migration CR above.
	c.mu.Lock()
	c.activeMigrations["default/pod-failed"] = &migrationEntry{
		name:       "pod-failed",
		namespace:  "default",
		createdAt:  time.Now().Add(-time.Hour),
		migration:  "mig-failed",
		retryCount: 0,
	}
	c.activeMigrations["default/pod-running"] = &migrationEntry{
		name:       "pod-running",
		namespace:  "default",
		createdAt:  time.Now(),
		migration:  "mig-running",
		retryCount: 0,
	}
	c.mu.Unlock()

	// The injected create failures are expected; discard the error logs
	// they would otherwise flood the test output with.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prev)

	pods := []*detector.PodPendingInfo{
		{Namespace: "default", PodName: "pod-failed", NodeName: "node-1"},
		{Namespace: "default", PodName: "pod-running", NodeName: "node-1"},
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: Trigger loops through handleExistingMigration, which for
	// the Failed migration writes retryCount/createdAt under c.mu on
	// every attempt (the subsequent create fails, so the entry keeps
	// pointing at mig-failed and the writes keep flowing).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c.Trigger(context.Background(), pods)
		}
	}()

	// Reader: cleanup loops snapshot the entries and read the mutable
	// fields — this is where the race used to fire.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.CleanupCompletedMigrations(context.Background())
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
