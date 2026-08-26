package detector

import (
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	pod := podWithResizePending("Deferred", "test", migrationLabels())

	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected pod not added before threshold")
	}

	time.Sleep(100 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected pod added after threshold, got %d", len(d.pendingPods))
	}
}

func TestOnPodChangeInfeasibleAddedImmediately(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})
	pod := podWithResizePending("Infeasible", "Node didn't have enough capacity", migrationLabels())

	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected Infeasible pod added immediately, got %d pending pods", len(d.pendingPods))
	}
}

func TestOnPodChangeNoLongerPending(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
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

func TestListSuspectPodsInfeasibleAlwaysSuspect(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-1 * time.Second)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   2500,
		Reason:       "Infeasible",
		Message:      "Node didn't have enough capacity",
		PendingSince: time.Now().Add(-1 * time.Second),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect for Infeasible, got %d", len(suspects))
	}
	if suspects[0].Reason != "Infeasible" {
		t.Fatalf("expected reason=Infeasible, got %s", suspects[0].Reason)
	}
}

func TestListSuspectPodsDeferredBelowThresholdNotSuspect(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})

	key := "default/resize-test"
	d.firstSeen[key] = time.Now()
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   1500,
		Reason:       "Deferred",
		Message:      "Node didn't have enough resource",
		PendingSince: time.Now(),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected 0 suspects for Deferred below threshold, got %d", len(suspects))
	}
}

func TestListSuspectPodsDeferredAboveThresholdIsSuspect(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-1 * time.Hour)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   1500,
		Reason:       "Deferred",
		Message:      "Node didn't have enough resource",
		PendingSince: time.Now().Add(-1 * time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect for Deferred above threshold, got %d", len(suspects))
	}
	if suspects[0].Reason != "Deferred" {
		t.Fatalf("expected reason=Deferred, got %s", suspects[0].Reason)
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

func TestListSuspectPodsMixedInfeasibleAndDeferred(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})

	// Infeasible — should be returned immediately
	d.firstSeen["default/pod-a"] = time.Now()
	d.pendingPods["default/pod-a"] = &PodPendingInfo{
		Namespace: "default", PodName: "pod-a", NodeName: "node-1",
		Reason: "Infeasible", PendingSince: time.Now(),
	}

	// Deferred below threshold — should NOT be returned
	d.firstSeen["default/pod-b"] = time.Now()
	d.pendingPods["default/pod-b"] = &PodPendingInfo{
		Namespace: "default", PodName: "pod-b", NodeName: "node-1",
		Reason: "Deferred", PendingSince: time.Now(),
	}

	// Deferred above threshold — should be returned
	d.firstSeen["default/pod-c"] = time.Now().Add(-1 * time.Hour)
	d.pendingPods["default/pod-c"] = &PodPendingInfo{
		Namespace: "default", PodName: "pod-c", NodeName: "node-2",
		Reason: "Deferred", PendingSince: time.Now().Add(-1 * time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 2 {
		t.Fatalf("expected 2 suspects (1 Infeasible + 1 Deferred above threshold), got %d", len(suspects))
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
					// AllocatedResources is nil, but Resources is set
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

	// First: Deferred — should NOT be added (below threshold)
	pod := podWithResizePending("Deferred", "test", migrationLabels())
	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatal("expected 0 pending pods for Deferred below threshold")
	}

	// Second: same pod but now Infeasible — should be added immediately
	pod.Status.Conditions[0].Reason = "Infeasible"
	pod.Status.Conditions[0].Message = "Node didn't have enough capacity"
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected 1 pending pod after transition to Infeasible, got %d", len(d.pendingPods))
	}
}
