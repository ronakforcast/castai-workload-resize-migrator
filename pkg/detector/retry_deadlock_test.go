package detector

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// podWithResizePendingAt builds a CLM-eligible pod whose PodResizePending
// condition carries a specific lastTransitionTime, mirroring what kubelet
// writes when a resize becomes stuck.
func podWithResizePendingAt(name string, stuckAt time.Time) *corev1.Pod {
	pod := podWithResizePending("Deferred", "node full", scopedMigrationLabels())
	pod.Name = name
	pod.Status.Conditions[0].LastTransitionTime = metav1.NewTime(stuckAt)
	return pod
}

// TestListSuspectPodsUsesConditionTimestamp is the regression test for the
// retry deadlock: a controller restart wipes the in-memory firstSeen map, so
// a Deferred pod whose migration failed after the restart must still pass
// the PendingThreshold via the PodResizePending condition's
// lastTransitionTime (kubelet-maintained, restart-surviving). Before the
// fix, pendingSince defaulted to "now" on every scan and the retry never
// fired.
func TestListSuspectPodsUsesConditionTimestamp(t *testing.T) {
	stuckAt := time.Now().Add(-10 * time.Minute) // stuck well past threshold
	cs := fake.NewSimpleClientset(podWithResizePendingAt("app-1", stuckAt))
	d := New(cs, config.Config{PendingThreshold: 2 * time.Minute})

	// No OnPodChange was ever invoked: the in-memory firstSeen map is empty,
	// exactly like a freshly restarted controller.
	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect via condition timestamp, got %d", len(suspects))
	}
	if since := suspects[0].PendingSince; since.Sub(stuckAt) > time.Minute {
		t.Fatalf("expected PendingSince near condition lastTransitionTime %v, got %v", stuckAt, since)
	}
}

// TestListSuspectPodsRecentConditionNotEligible verifies the guard on the
// same fix: a Deferred pod whose condition says it just became stuck (and
// which the controller has never seen via the informer) must NOT be eligible
// yet — the threshold still applies.
func TestListSuspectPodsRecentConditionNotEligible(t *testing.T) {
	stuckAt := time.Now().Add(-10 * time.Second) // just now
	cs := fake.NewSimpleClientset(podWithResizePendingAt("app-2", stuckAt))
	d := New(cs, config.Config{PendingThreshold: 2 * time.Minute})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 0 {
		t.Fatalf("expected 0 suspects for recently-stuck pod, got %d", len(suspects))
	}
}

// alwaysActiveChecker reports every pod as actively migrating.
type alwaysActiveChecker struct{}

func (alwaysActiveChecker) IsActive(namespace, podName string) bool { return true }

// TestOnPodChangeRecordsFirstSeenForActivePod is the regression test for
// the deadlock's other half: OnPodChange must record firstSeen BEFORE the
// active-migration early return. Otherwise a pod with an in-flight
// migration never enters the pending bookkeeping, and after a restart the
// safety scan cannot see how long it has been stuck.
func TestOnPodChangeRecordsFirstSeenForActivePod(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0})
	d.SetMigrationStateChecker(alwaysActiveChecker{})

	pod := podWithResizePending("Deferred", "node full", scopedMigrationLabels())
	d.OnPodChange(pod) // must be a no-op trigger-wise (active migration)...

	key := pod.Namespace + "/" + pod.Name
	d.mu.RLock()
	_, seen := d.firstSeen[key]
	d.mu.RUnlock()
	if !seen {
		t.Fatal("expected firstSeen to be recorded even for pods with an active migration")
	}

	// ...and a second OnPodChange must not re-trigger the migrate callback
	// (the H2 behavior is preserved).
	rec := &fakeMigrateRecorder{}
	d.SetMigrateFunc(rec.record)
	d.OnPodChange(pod)
	if len(rec.pods) != 0 {
		t.Fatalf("expected no migrate callback for active pod, got %d", len(rec.pods))
	}
}
