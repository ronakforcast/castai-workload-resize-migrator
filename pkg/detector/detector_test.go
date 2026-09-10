package detector

import (
	"context"
	"sync"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

func podWithResizePending(reason, message string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1500m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:    corev1.PodResizePending,
					Status:  corev1.ConditionTrue,
					Reason:  reason,
					Message: message,
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
	}
}

func migrationLabels() map[string]string {
	return map[string]string{
		"live.cast.ai/migration-enabled": "true",
	}
}

// fakeMigrateFunc captures calls to the migrate callback.
type fakeMigrateFunc struct {
	mu    sync.Mutex
	calls []*PodPendingInfo
	err   error
}

func (f *fakeMigrateFunc) fn(ctx context.Context, p *PodPendingInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, p)
	return f.err
}

func (f *fakeMigrateFunc) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestExtractResizeStatusDeferred(t *testing.T) {
	d := New(nil, config.Config{})
	pod := podWithResizePending("Deferred", "Node didn't have enough resource: cpu, requested: 1500, used: 1450, capacity: 1930", migrationLabels())

	reason, message, pending := d.extractResizeStatus(pod)
	if !pending {
		t.Fatal("expected pending=true")
	}
	if reason != "Deferred" {
		t.Fatalf("expected reason=Deferred, got %s", reason)
	}
	if message == "" {
		t.Fatal("expected non-empty message")
	}
}

func TestExtractResizeStatusInfeasible(t *testing.T) {
	d := New(nil, config.Config{})
	pod := podWithResizePending("Infeasible", "Node didn't have enough capacity: cpu, requested: 2500, capacity: 1930", migrationLabels())

	reason, message, pending := d.extractResizeStatus(pod)
	if !pending {
		t.Fatal("expected pending=true")
	}
	if reason != "Infeasible" {
		t.Fatalf("expected reason=Infeasible, got %s", reason)
	}
	if message == "" {
		t.Fatal("expected non-empty message")
	}
}

func TestExtractResizeStatusNotPending(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    migrationLabels(),
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			},
		},
	}

	_, _, pending := d.extractResizeStatus(pod)
	if pending {
		t.Fatal("expected pending=false for pod without PodResizePending condition")
	}
}

func TestExtractResizeStatusConditionFalse(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    migrationLabels(),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodResizePending,
					Status: corev1.ConditionFalse,
					Reason: "Deferred",
				},
			},
		},
	}

	_, _, pending := d.extractResizeStatus(pod)
	if pending {
		t.Fatal("expected pending=false when condition status is False")
	}
}

func TestOnPodChangeFiltersNonMigrationLabel(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	pod := podWithResizePending("Deferred", "test", map[string]string{"app": "nginx"})

	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod)

	if len(d.pendingPods) != 0 {
		t.Fatalf("expected 0 pending pods for non-migration-enabled pod, got %d", len(d.pendingPods))
	}
}

func TestOnPodChangeDeferredWaitsForThreshold(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 50 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := podWithResizePending("Deferred", "test", migrationLabels())

	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected pod not added before threshold")
	}
	if fake.callCount() != 0 {
		t.Fatal("expected no migration call before threshold")
	}

	time.Sleep(100 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected pod added after threshold, got %d", len(d.pendingPods))
	}
	if fake.callCount() != 1 {
		t.Fatalf("expected 1 migration call after threshold, got %d", fake.callCount())
	}
}

func TestOnPodChangeInfeasibleTriggersImmediately(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := podWithResizePending("Infeasible", "Node didn't have enough capacity", migrationLabels())

	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected Infeasible pod added immediately, got %d pending pods", len(d.pendingPods))
	}
	if fake.callCount() != 1 {
		t.Fatalf("expected 1 migration call immediately for Infeasible, got %d", fake.callCount())
	}
}

func TestOnPodChangeInfeasibleNoCallback(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})
	// No migrate func set — should not panic, pod still added to pending.

	pod := podWithResizePending("Infeasible", "Node didn't have enough capacity", migrationLabels())

	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected Infeasible pod added immediately, got %d pending pods", len(d.pendingPods))
	}
}

func TestOnPodChangeNoLongerPending(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := podWithResizePending("Deferred", "test", migrationLabels())

	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected pod pending, got %d", len(d.pendingPods))
	}

	// Resize applied — remove PodResizePending condition.
	pod.Status.Conditions = []corev1.PodCondition{}
	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatalf("expected pod removed after resize applied, got %d", len(d.pendingPods))
	}
}

func TestOnPodDelete(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := podWithResizePending("Deferred", "test", migrationLabels())

	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatal("expected pod pending")
	}

	d.OnPodDelete(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected pod removed on delete")
	}
}

func TestListSuspectPodsReturnsAllPending(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})

	d.firstSeen["default/pod-a"] = time.Now().Add(-1 * time.Hour)
	d.pendingPods["default/pod-a"] = &PodPendingInfo{
		Namespace: "default", PodName: "pod-a", NodeName: "node-1",
		Reason: "Infeasible", PendingSince: time.Now().Add(-1 * time.Hour),
	}
	d.firstSeen["default/pod-b"] = time.Now().Add(-1 * time.Hour)
	d.pendingPods["default/pod-b"] = &PodPendingInfo{
		Namespace: "default", PodName: "pod-b", NodeName: "node-2",
		Reason: "Deferred", PendingSince: time.Now().Add(-1 * time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 2 {
		t.Fatalf("expected 2 suspects, got %d", len(suspects))
	}
}

func TestListSuspectPodsEmpty(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected 0 suspects, got %d", len(suspects))
	}
}

func TestResolveWorkload(t *testing.T) {
	d := New(nil, config.Config{})

	tests := []struct {
		name    string
		pod     *corev1.Pod
		expName string
		expKind string
	}{
		{
			name: "Deployment pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"pod-template-hash": "abc123"},
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "ReplicaSet", Name: "my-deploy-abc123"},
					},
				},
			},
			expName: "my-deploy",
			expKind: "Deployment",
		},
		{
			name: "StatefulSet pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{Kind: "StatefulSet", Name: "my-stateful"},
					},
				},
			},
			expName: "my-stateful",
			expKind: "StatefulSet",
		},
		{
			name: "Standalone pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "lonely-pod"},
			},
			expName: "lonely-pod",
			expKind: "Pod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, kind := d.resolveWorkload(tt.pod)
			if name != tt.expName || kind != tt.expKind {
				t.Fatalf("expected %s/%s, got %s/%s", tt.expKind, tt.expName, kind, name)
			}
		})
	}
}

func TestExtractCPUValues(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1500m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			},
		},
	}

	desired, allocated := d.extractCPUValues(pod)
	if desired != 1500 {
		t.Fatalf("expected desired=1500, got %d", desired)
	}
	if allocated != 100 {
		t.Fatalf("expected allocated=100, got %d", allocated)
	}
}

func TestExtractCPUValuesMultiContainer(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("800m"),
						},
					},
				},
				{
					Name: "sidecar",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("200m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
				{
					Name: "sidecar",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("50m"),
					},
				},
			},
		},
	}

	desired, allocated := d.extractCPUValues(pod)
	if desired != 1000 {
		t.Fatalf("expected desired=1000, got %d", desired)
	}
	if allocated != 150 {
		t.Fatalf("expected allocated=150, got %d", allocated)
	}
}

func TestOnPodChangeNilPod(t *testing.T) {
	d := New(nil, config.Config{})
	d.OnPodChange(nil)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected 0 pending pods for nil pod")
	}
}

func TestOnPodChangeNilLabels(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodResizePending, Status: corev1.ConditionTrue, Reason: "Deferred"},
			},
		},
	}
	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected 0 pending pods for pod with nil labels")
	}
}

func TestOnPodChangeMultipleConditions(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
			Labels:    migrationLabels(),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodResizePending, Status: corev1.ConditionTrue, Reason: "Deferred", Message: "test"},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected 1 pending pod with multiple conditions, got %d", len(d.pendingPods))
	}
}

func TestExtractCPUValuesMissingAllocatedResources(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("500m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
	}

	desired, allocated := d.extractCPUValues(pod)
	if desired != 500 {
		t.Fatalf("expected desired=500, got %d", desired)
	}
	if allocated != 100 {
		t.Fatalf("expected allocated=100 (fallback to Resources), got %d", allocated)
	}
}

func TestOnPodDeleteNilPod(t *testing.T) {
	d := New(nil, config.Config{})
	d.OnPodDelete(nil)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected 0 pending pods after nil delete")
	}
}

func TestOnPodChangeReasonTransitionsFromDeferredToInfeasible(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	// First: Deferred — should NOT trigger (below threshold)
	pod := podWithResizePending("Deferred", "test", migrationLabels())
	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected 0 pending pods for Deferred below threshold")
	}
	if fake.callCount() != 0 {
		t.Fatal("expected no migration call for Deferred below threshold")
	}

	// Second: same pod but now Infeasible — should trigger immediately
	pod.Status.Conditions[0].Reason = "Infeasible"
	pod.Status.Conditions[0].Message = "Node didn't have enough capacity"
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected 1 pending pod after transition to Infeasible, got %d", len(d.pendingPods))
	}
	if fake.callCount() != 1 {
		t.Fatalf("expected 1 migration call for Infeasible, got %d", fake.callCount())
	}
}

func TestSetMigrateFunc(t *testing.T) {
	d := New(nil, config.Config{})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	if d.migrateFunc == nil {
		t.Fatal("expected migrateFunc to be set")
	}
}

// podWithResizePendingScoped builds a PodResizePending pod with a custom
// name/namespace and node so cluster-scan tests can stage multiple pods.
func podWithResizePendingScoped(name, namespace, node string, reason, message string, labels map[string]string) *corev1.Pod {
	p := podWithResizePending(reason, message, labels)
	p.Name = name
	p.Namespace = namespace
	p.Spec.NodeName = node
	// Field selector "status.phase=Running" is applied by the safety scan,
	// so the pods we stage for cluster-scan tests must be Running.
	p.Status.Phase = corev1.PodRunning
	return p
}

func TestListSuspectPodsScansClusterWhenMemoryEmpty(t *testing.T) {
	// In-memory map is empty; a PodResizePending pod exists only in the
	// fake clientset. The safety scan must still find it.
	clientset := fake.NewSimpleClientset(
		podWithResizePendingScoped("scan-pod-a", "default", "node-1", "Infeasible", "no capacity", migrationLabels()),
	)
	d := New(clientset, config.Config{PendingThreshold: 1 * time.Minute})

	if len(d.pendingPods) != 0 {
		t.Fatal("precondition: pendingPods should be empty")
	}

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect from cluster scan, got %d", len(suspects))
	}
	if suspects[0].PodName != "scan-pod-a" || suspects[0].Namespace != "default" {
		t.Fatalf("unexpected suspect: %+v", suspects[0])
	}
	if suspects[0].Reason != "Infeasible" {
		t.Fatalf("expected reason=Infeasible, got %q", suspects[0].Reason)
	}
	if suspects[0].DesiredCPU != 1500 || suspects[0].AllocatedCPU != 100 {
		t.Fatalf("expected desired=1500, allocated=100, got %+v", suspects[0])
	}
}

func TestListSuspectPodsDeferredBelowThresholdSkipped(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		podWithResizePendingScoped("scan-pod-b", "default", "node-1", "Deferred", "waiting", migrationLabels()),
	)
	d := New(clientset, config.Config{PendingThreshold: 10 * time.Minute})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 0 {
		t.Fatalf("expected Deferred pod below threshold to be skipped, got %d suspects", len(suspects))
	}
}

func TestListSuspectPodsDeferredAboveThresholdIncluded(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		podWithResizePendingScoped("scan-pod-c", "default", "node-1", "Deferred", "waiting", migrationLabels()),
	)
	d := New(clientset, config.Config{PendingThreshold: 10 * time.Millisecond})

	// Seed firstSeen so PendingSince predates the threshold — mirrors a
	// pod that has been observed by OnPodChange for a while.
	d.firstSeen["default/scan-pod-c"] = time.Now().Add(-1 * time.Hour)

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected Deferred pod above threshold to be included, got %d suspects", len(suspects))
	}
	if suspects[0].PodName != "scan-pod-c" {
		t.Fatalf("unexpected suspect: %+v", suspects[0])
	}
}

func TestListSuspectPodsUsesFirstSeenForPendingSince(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		podWithResizePendingScoped("scan-pod-d", "default", "node-1", "Deferred", "waiting", migrationLabels()),
	)
	d := New(clientset, config.Config{PendingThreshold: 10 * time.Millisecond})

	// firstSeen predates now — PendingSince should be carried over
	// from the in-memory map rather than set to time.Now().
	stamp := time.Now().Add(-1 * time.Hour)
	d.firstSeen["default/scan-pod-d"] = stamp

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect, got %d", len(suspects))
	}
	if !suspects[0].PendingSince.Equal(stamp) {
		t.Fatalf("expected PendingSince to use firstSeen stamp %v, got %v", stamp, suspects[0].PendingSince)
	}
}

func TestListSuspectPodsIgnoresUnlabeledPods(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		podWithResizePendingScoped("scan-pod-e", "default", "node-1", "Infeasible", "no capacity", map[string]string{"app": "nginx"}),
		podWithResizePendingScoped("scan-pod-f", "default", "node-2", "Infeasible", "no capacity", migrationLabels()),
	)
	d := New(clientset, config.Config{PendingThreshold: 1 * time.Minute})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected only the labeled pod to be returned, got %d suspects", len(suspects))
	}
	if suspects[0].PodName != "scan-pod-f" {
		t.Fatalf("expected scan-pod-f, got %q", suspects[0].PodName)
	}
}

func TestListSuspectPodsIgnoresNonPendingPods(t *testing.T) {
	notPending := podWithResizePendingScoped("scan-pod-g", "default", "node-1", "Deferred", "waiting", migrationLabels())
	notPending.Status.Conditions = []corev1.PodCondition{} // no PodResizePending
	clientset := fake.NewSimpleClientset(notPending)
	d := New(clientset, config.Config{PendingThreshold: 1 * time.Minute})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 0 {
		t.Fatalf("expected non-pending pod to be skipped, got %d suspects", len(suspects))
	}
}

func TestSafetyScanUsesSelectors(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	d := New(clientset, config.Config{PendingThreshold: 1 * time.Minute})

	if got := d.ListSuspectPods(context.Background()); len(got) != 0 {
		t.Fatalf("expected empty result from empty clientset, got %d suspects", len(got))
	}

	var listAction clientgotesting.ListAction
	for _, a := range clientset.Actions() {
		la, ok := a.(clientgotesting.ListAction)
		if !ok {
			continue
		}
		if la.GetResource().Resource != "pods" {
			continue
		}
		listAction = la
	}
	if listAction == nil {
		t.Fatal("expected a pod list action recorded by the fake clientset")
	}

	restrictions := listAction.GetListRestrictions()

	const wantLabel = "live.cast.ai/migration-enabled=true"
	const wantField = "status.phase=Running"

	if got := restrictions.Labels.String(); got != wantLabel {
		t.Errorf("label selector: got %q, want %q", got, wantLabel)
	}
	if got := restrictions.Fields.String(); got != wantField {
		t.Errorf("field selector: got %q, want %q", got, wantField)
	}
}
