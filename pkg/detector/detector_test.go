package detector

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestExtractResizeStatus(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "stress",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("800m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "stress",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
		},
	}

	desired, allocated, pending := d.extractResizeStatus(pod)
	if !pending {
		t.Fatalf("expected pending resize")
	}
	if desired != 800 {
		t.Fatalf("expected desired 800m, got %d", desired)
	}
	if allocated != 100 {
		t.Fatalf("expected allocated 100m, got %d", allocated)
	}
}

func TestExtractResizeStatusNoPending(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "stress",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "stress",
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

	desired, allocated, pending := d.extractResizeStatus(pod)
	if pending {
		t.Fatalf("expected no pending resize, got desired=%d allocated=%d", desired, allocated)
	}
	if desired != 0 || allocated != 0 {
		t.Fatalf("expected desired=0 allocated=0, got %d/%d", desired, allocated)
	}
}

func TestExtractResizeStatusResizeApplied(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "stress",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("400m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "stress",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("400m"),
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("400m"),
						},
					},
				},
			},
		},
	}

	desired, allocated, pending := d.extractResizeStatus(pod)
	if pending {
		t.Fatalf("expected no pending resize after applied, got desired=%d allocated=%d", desired, allocated)
	}
}

func TestExtractResizeStatusMultiContainer(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("300m"),
						},
					},
				},
				{
					Name: "sidecar",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("100m"),
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

	desired, allocated, pending := d.extractResizeStatus(pod)
	if !pending {
		t.Fatal("expected pending resize")
	}
	if desired != 400 {
		t.Fatalf("expected desired=400, got %d", desired)
	}
	if allocated != 150 {
		t.Fatalf("expected allocated=150, got %d", allocated)
	}
}

func TestExtractResizeStatusDownsizeIgnored(t *testing.T) {
	d := New(nil, config.Config{})
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "stress",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("50m"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "stress",
					AllocatedResources: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			},
		},
	}

	_, _, pending := d.extractResizeStatus(pod)
	if pending {
		t.Fatal("expected downsize to be ignored")
	}
}

func TestResolveWorkload(t *testing.T) {
	d := New(nil, config.Config{})

	tests := []struct {
		name     string
		pod      *corev1.Pod
		expName  string
		expKind  string
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

func TestOnPodChangePendingThreshold(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 50 * time.Millisecond})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    map[string]string{"live.cast.ai/migration-enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("400m"),
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

func TestOnPodChangeNoLongerPending(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    map[string]string{"live.cast.ai/migration-enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("400m"),
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

	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod)
	if len(d.pendingPods) != 1 {
		t.Fatalf("expected pod pending, got %d", len(d.pendingPods))
	}

	// Resize applied.
	pod.Status.ContainerStatuses[0].AllocatedResources = corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("400m"),
	}
	d.OnPodChange(pod)
	if len(d.pendingPods) != 0 {
		t.Fatalf("expected pod removed after resize applied, got %d", len(d.pendingPods))
	}
}

func TestOnPodDelete(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resize-test",
			Namespace: "default",
			Labels:    map[string]string{"live.cast.ai/migration-enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("400m")},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app",
					AllocatedResources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				},
			},
		},
	}

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

func TestListSuspectPodsDeltaBelowThreshold(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   1 * time.Millisecond,
		NodeDeltaThreshold: 0.15,
	})

	d.nodes["node-1"] = &NodeInfo{Name: "node-1", AllocatableCPU: 1930, Pods: make(map[string]*PodPendingInfo)}
	d.nodePodSum["node-1"] = 1800

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-time.Hour)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   110,
		PendingSince: time.Now().Add(-time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected no suspects below threshold, got %v", suspects)
	}
}

func TestListSuspectPodsHasEnoughRoom(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   1 * time.Millisecond,
		NodeDeltaThreshold: 0.10,
	})

	d.nodes["node-1"] = &NodeInfo{Name: "node-1", AllocatableCPU: 1930, Pods: make(map[string]*PodPendingInfo)}
	d.nodePodSum["node-1"] = 1000

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-time.Hour)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   400,
		PendingSince: time.Now().Add(-time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected no suspects when node has room, got %v", suspects)
	}
}

func TestListSuspectPodsMultiplePods(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   1 * time.Millisecond,
		NodeDeltaThreshold: 0.10,
	})

	d.nodes["node-1"] = &NodeInfo{Name: "node-1", AllocatableCPU: 1930, Pods: make(map[string]*PodPendingInfo)}
	d.nodePodSum["node-1"] = 2100

	for i, key := range []string{"default/pod-a", "default/pod-b"} {
		d.firstSeen[key] = time.Now().Add(-time.Hour)
		d.pendingPods[key] = &PodPendingInfo{
			Namespace:    "default",
			PodName:      []string{"pod-a", "pod-b"}[i],
			NodeName:     "node-1",
			AllocatedCPU: 100,
			DesiredCPU:   250,
			PendingSince: time.Now().Add(-time.Hour),
		}
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 2 {
		t.Fatalf("expected 2 suspects, got %v", suspects)
	}
}

func TestListSuspectPodsNodeAllocatableZero(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   1 * time.Millisecond,
		NodeDeltaThreshold: 0.10,
	})

	d.nodes["node-1"] = &NodeInfo{Name: "node-1", AllocatableCPU: 0, Pods: make(map[string]*PodPendingInfo)}
	d.nodePodSum["node-1"] = 0

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-time.Hour)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   400,
		PendingSince: time.Now().Add(-time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected no suspects with zero allocatable, got %v", suspects)
	}
}

func TestListSuspectPodsUnknownNode(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   1 * time.Millisecond,
		NodeDeltaThreshold: 0.10,
	})

	key := "default/resize-test"
	d.firstSeen[key] = time.Now().Add(-time.Hour)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "default",
		PodName:      "resize-test",
		NodeName:     "unknown-node",
		AllocatedCPU: 100,
		DesiredCPU:   400,
		PendingSince: time.Now().Add(-time.Hour),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 0 {
		t.Fatalf("expected no suspects for unknown node, got %v", suspects)
	}
}

func TestRefreshNodePodSum(t *testing.T) {
	fakeClient := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "running-1", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "running-2", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")}}},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}}},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	)

	d := New(fakeClient, config.Config{})
	if err := d.RefreshNodePodSum(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if d.nodePodSum["node-1"] != 300 {
		t.Fatalf("expected node-1 sum=300, got %d", d.nodePodSum["node-1"])
	}
}

func TestListSuspectPods(t *testing.T) {
	d := New(nil, config.Config{
		PendingThreshold:   30 * time.Second,
		NodeDeltaThreshold: 0.15,
	})

	d.nodes["node-1"] = &NodeInfo{
		Name:           "node-1",
		AllocatableCPU: 1930,
		Pods:           make(map[string]*PodPendingInfo),
	}
	d.nodePodSum["node-1"] = 950 + 100 // other pods + target

	key := "woop-test/cpu-stress-target"
	d.firstSeen[key] = time.Now().Add(-60 * time.Second)
	d.pendingPods[key] = &PodPendingInfo{
		Namespace:    "woop-test",
		PodName:      "cpu-stress-target",
		WorkloadName: "cpu-stress-target",
		WorkloadKind: "Deployment",
		NodeName:     "node-1",
		AllocatedCPU: 100,
		DesiredCPU:   2000,
		PendingSince: time.Now().Add(-60 * time.Second),
	}

	suspects := d.ListSuspectPods(nil)
	if len(suspects) != 1 || suspects[0].PodName != "cpu-stress-target" {
		t.Fatalf("expected cpu-stress-target as suspect, got %v", suspects)
	}
}
