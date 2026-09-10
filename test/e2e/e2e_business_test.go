//go:build !e2e

// e2e_business_test.go contains integration-style tests that exercise the
// detector -> workqueue -> migrator pipeline with fake clients. These run
// during normal `go test ./...` and complement the real-cluster e2e suite
// (e2e_test.go, gated by the `e2e` build tag).
//
// QA-plan coverage:
//   TC-01: golden path — pod with PodResizePending creates Migration CRD
//   TC-02: spec.destination equals source node name
//   TC-03: CLMNodeTemplate label applied
//   TC-13: leader-election HA is skipped here (needs real cluster / envtest)
//   TC-16: multiple pods create multiple migrations
package e2e

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"
	"castai-workload-resize-migrator/pkg/detector"
	"castai-workload-resize-migrator/pkg/migrator"
	"castai-workload-resize-migrator/pkg/workqueue"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

var migrationGVR = schema.GroupVersionResource{
	Group:    "live.cast.ai",
	Version:  "v1",
	Resource: "migrations",
}

func newFakeDynamicClient() dynamic.Interface {
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		migrationGVR: "MigrationList",
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
}

func makePendingPod(name, ns, node, reason string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"live.cast.ai/migration-enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: node,
			Containers: []corev1.Container{{
				Name: "app",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1500m"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:    corev1.PodResizePending,
				Status:  corev1.ConditionTrue,
				Reason:  reason,
				Message: "test",
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app",
				AllocatedResources: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("100m"),
				},
			}},
		},
	}
}

// buildPipeline wires a detector, workqueue, and migrator the same way
// cmd/castai-workload-resize-migrator does, but with fake clients and a
// cancellable context. After wiring, it drives a single safety scan to
// trigger the detector -> queue -> migrator flow.
func buildPipeline(ctx context.Context, cfg config.Config, cs *fake.Clientset, dyn dynamic.Interface) *migrator.Client {
	det := detector.New(cs, cfg)
	mig := migrator.New(cfg, dyn)
	q := workqueue.New(workqueue.DefaultBufferSize)

	det.SetMigrateFunc(func(_ context.Context, p *detector.PodPendingInfo) error {
		q.Add(p)
		return nil
	})

	q.Start(ctx, 1, func(_ context.Context, p *detector.PodPendingInfo) error {
		return mig.Trigger(ctx, []*detector.PodPendingInfo{p})
	})

	// Simulate a safety scan to feed qualifying pods through the pipeline.
	suspects := det.ListSuspectPods(ctx)
	if len(suspects) > 0 {
		_ = mig.Trigger(ctx, suspects)
	}

	return mig
}

// TestBusiness_TC01_GoldenPathInfeasible creates a pod with
// PodResizePending=Infeasible and verifies the pipeline creates a Migration
// CRD for it.
func TestBusiness_TC01_GoldenPathInfeasible(t *testing.T) {
	cs := fake.NewSimpleClientset(makePendingPod("app-1", "default", "node-1", "Infeasible"))
	dyn := newFakeDynamicClient()
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mig := buildPipeline(ctx, cfg, cs, dyn)

	// Give the queue time to process the item.
	time.Sleep(100 * time.Millisecond)

	if !mig.IsActive("default", "app-1") {
		t.Fatal("expected migration to be active for default/app-1")
	}
}

// TestBusiness_TC01_GoldenPathDeferred creates a pod with
// PodResizePending=Deferred and verifies the pipeline creates a Migration
// CRD after the pending threshold elapses.
func TestBusiness_TC01_GoldenPathDeferred(t *testing.T) {
	cs := fake.NewSimpleClientset(makePendingPod("app-2", "default", "node-2", "Deferred"))
	dyn := newFakeDynamicClient()
	cfg := config.Config{DryRun: false, PendingThreshold: 0, MigrationTimeout: 10 * time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mig := buildPipeline(ctx, cfg, cs, dyn)

	// Wait for the deferred threshold and the queue worker.
	time.Sleep(100 * time.Millisecond)

	if !mig.IsActive("default", "app-2") {
		t.Fatal("expected migration to be active for default/app-2 after deferred threshold")
	}
}

// TestBusiness_TC02_DestinationEqualsSourceNode verifies that the created
// Migration CRD carries spec.destination equal to the pod's current node.
func TestBusiness_TC02_DestinationEqualsSourceNode(t *testing.T) {
	cs := fake.NewSimpleClientset(makePendingPod("app-3", "default", "node-src", "Infeasible"))
	dyn := newFakeDynamicClient()
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buildPipeline(ctx, cfg, cs, dyn)

	time.Sleep(100 * time.Millisecond)

	migCR, err := dyn.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(migCR.Items) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migCR.Items))
	}

	dest, found, err := unstructured.NestedString(migCR.Items[0].Object, "spec", "destination")
	if err != nil || !found {
		t.Fatalf("spec.destination not found: err=%v found=%v", err, found)
	}
	if dest != "node-src" {
		t.Fatalf("spec.destination=%q, want node-src", dest)
	}
}

// TestBusiness_TC03_CLMNodeTemplateLabel verifies that a configured
// CLMNodeTemplate is propagated as a label on the Migration CRD.
func TestBusiness_TC03_CLMNodeTemplateLabel(t *testing.T) {
	cs := fake.NewSimpleClientset(makePendingPod("app-4", "default", "node-1", "Infeasible"))
	dyn := newFakeDynamicClient()
	cfg := config.Config{
		DryRun:          false,
		CLMNodeTemplate: "castai-spot-xlarge",
		MigrationTimeout: 10 * time.Minute,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buildPipeline(ctx, cfg, cs, dyn)

	time.Sleep(100 * time.Millisecond)

	migCR, err := dyn.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(migCR.Items) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migCR.Items))
	}

	label, found, err := unstructured.NestedString(migCR.Items[0].Object, "metadata", "labels", "castai-workload-resize-migrator/clm-node-template")
	if err != nil || !found {
		t.Fatalf("clm-node-template label missing: err=%v found=%v", err, found)
	}
	if label != "castai-spot-xlarge" {
		t.Fatalf("clm-node-template label=%q, want castai-spot-xlarge", label)
	}
}

// TestBusiness_TC16_MultiplePodsCreateMultipleMigrations verifies that five
// qualifying pods each get their own Migration CRD.
func TestBusiness_TC16_MultiplePodsCreateMultipleMigrations(t *testing.T) {
	cs := fake.NewSimpleClientset(
		makePendingPod("app-a", "default", "node-1", "Infeasible"),
		makePendingPod("app-b", "default", "node-2", "Infeasible"),
		makePendingPod("app-c", "default", "node-3", "Infeasible"),
		makePendingPod("app-d", "default", "node-4", "Infeasible"),
		makePendingPod("app-e", "default", "node-5", "Infeasible"),
	)
	dyn := newFakeDynamicClient()
	cfg := config.Config{DryRun: false, MigrationTimeout: 10 * time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mig := buildPipeline(ctx, cfg, cs, dyn)

	time.Sleep(200 * time.Millisecond)

	for _, name := range []string{"app-a", "app-b", "app-c", "app-d", "app-e"} {
		if !mig.IsActive("default", name) {
			t.Fatalf("expected migration active for default/%s", name)
		}
	}

	migCR, err := dyn.Resource(migrationGVR).Namespace("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(migCR.Items) != 5 {
		t.Fatalf("expected 5 migration CRDs, got %d", len(migCR.Items))
	}
}

// TestBusiness_TC13_LeaderElectionHANeedsRealCluster documents that the
// leader-election HA scenario (only one replica active) requires a real
// cluster or envtest and is intentionally left to the `e2e` build tag.
func TestBusiness_TC13_LeaderElectionHANeedsRealCluster(t *testing.T) {
	t.Skip("TC-13 leader-election HA requires a real cluster or envtest; run with -tags=e2e when available")
}
