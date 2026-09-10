package detector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// benchPod builds a pod for the safety-scan benchmark. eligible pods
// carry the CLM label; reason non-empty adds a PodResizePending=True
// condition with that reason.
func benchPod(name string, eligible bool, reason string) *corev1.Pod {
	labels := map[string]string{}
	if eligible {
		labels[migrationEnabledLabel] = "true"
	}
	conds := []corev1.PodCondition{}
	if reason != "" {
		conds = append(conds, corev1.PodCondition{
			Type:    podResizePendingType,
			Status:  corev1.ConditionTrue,
			Reason:  reason,
			Message: "bench",
		})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "bench", Labels: labels},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: conds,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "c",
				AllocatedResources: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("500m"),
				},
			}},
		},
	}
}

// BenchmarkListSuspectPods10k measures the fallback safety scan over a
// 10,000-pod cluster: 20% carry the migration-enabled label (of those,
// half Infeasible — immediately eligible — and half Deferred — below
// the PendingThreshold because firstSeen is unknown to a fresh
// controller), the remaining 80% are unlabeled and excluded. The scan
// must find exactly the 1,000 Infeasible pods.
//
// This is the benchmark referenced in issue #1 (P2: "Go benchmarks for
// the safety scan"). Run with:
//
//	go test ./pkg/detector -run '^$' -bench .
func BenchmarkListSuspectPods10k(b *testing.B) {
	ctx := context.Background()
	cs := fake.NewSimpleClientset()

	const total = 10000
	for i := 0; i < total; i++ {
		var pod *corev1.Pod
		switch {
		case i < 2000: // labeled candidates
			if i%2 == 0 {
				pod = benchPod(fmt.Sprintf("p-%d", i), true, "Infeasible")
			} else {
				pod = benchPod(fmt.Sprintf("p-%d", i), true, "Deferred")
			}
		default: // noise: unlabeled
			pod = benchPod(fmt.Sprintf("p-%d", i), false, "")
		}
		if _, err := cs.CoreV1().Pods("bench").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			b.Fatalf("create pod: %v", err)
		}
	}

	det := New(cs, config.Config{PendingThreshold: 2 * time.Minute})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		suspects := det.ListSuspectPods(ctx)
		if len(suspects) != 1000 {
			b.Fatalf("expected exactly 1000 eligible Infeasible suspects, got %d", len(suspects))
		}
	}
}
