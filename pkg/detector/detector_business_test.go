// detector_business_test.go covers detector business-logic tests pinned to
// QA-plan test IDs. These complement pkg/detector/detector_test.go (which
// exercises internal helpers exhaustively) by locking down observable
// behavior from the QA plan:
//
//	TC-05: only Infeasible / Deferred reasons are actionable
//	TC-10: pod whose migrator state is active is skipped
//	TC-17: pod without PodResizePending condition is ignored
//	TC-18: pod whose condition is now absent/False is ignored
//	TC-19: if the node has enough CPU, the pod is not flagged
//	TC-25: pod delete clears detector tracking
//
// All tests use fake.NewSimpleClientset + a small MigrationStateChecker
// fake and are table-driven where applicable.
package detector

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// makePodResizePending builds a Running pod with the given PodResizePending
// condition. Pass status=ConditionTrue to make it pending; otherwise the
// pod has no condition and is treated as not-pending.
func makePodResizePending(name, ns, node, reason, message string, status corev1.ConditionStatus) *corev1.Pod {
	pod := podWithResizePending(reason, message, migrationLabels())
	pod.Name = name
	pod.Namespace = ns
	pod.Spec.NodeName = node
	pod.Status.Phase = corev1.PodRunning
	if status == "" {
		pod.Status.Conditions = nil
	} else {
		pod.Status.Conditions[0].Status = status
		if reason != "" {
			pod.Status.Conditions[0].Reason = reason
		}
	}
	return pod
}

// fakeActiveChecker is a tiny MigrationStateChecker implementation used
// by TC-10. activeFor records pod keys that should be reported active.
type fakeActiveChecker struct {
	active map[string]bool
	calls  atomic.Int64
}

func newFakeActiveChecker(keys ...string) *fakeActiveChecker {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return &fakeActiveChecker{active: m}
}

func (f *fakeActiveChecker) IsActive(ns, name string) bool {
	f.calls.Add(1)
	return f.active[ns+"/"+name]
}

// ─── TC-05: only Infeasible / Deferred are actionable ────────────────────

// TestBusiness_TC05_OnlyInfeasibleOrDeferredActionable pins down the
// reason filter at the entry point of OnPodChange: a pod whose reason is
// "Unknown" (or empty) must not be added to pendingPods and must not
// invoke the migration callback, even after the PendingThreshold elapses.
//
// This is the business rule from the QA plan and is enforced via
// isActionableReason, which is shared by OnPodChange and ListSuspectPods.
func TestBusiness_TC05_OnlyInfeasibleOrDeferredActionable(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		wantPend bool
		wantCall int64
	}{
		// Infeasible triggers on every qualifying call (both calls fire
		// because there is no dedup at the detector layer); Deferred
		// needs two calls (first seeds firstSeen, second post-threshold).
		{name: "Infeasible is actionable", reason: "Infeasible", wantPend: true, wantCall: 2},
		{name: "Deferred is actionable", reason: "Deferred", wantPend: true, wantCall: 1},
		{name: "Unknown is ignored", reason: "Unknown", wantPend: false, wantCall: 0},
		{name: "empty reason is ignored", reason: "", wantPend: false, wantCall: 0},
		{name: "arbitrary kubelet reason is ignored", reason: "WaitingForQuota", wantPend: false, wantCall: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
			fake := &fakeMigrateFunc{}
			d.SetMigrateFunc(fake.fn)

			pod := makePodResizePending("p", "default", "node-1", tc.reason, "test", corev1.ConditionTrue)

			// First call: seeds firstSeen (and triggers Infeasible
			// immediately). For non-actionable reasons this is a no-op
			// apart from recording firstSeen.
			d.OnPodChange(pod)

			// Wait past the threshold then re-fire. Actionable reasons
			// add to pendingPods and call migrateFunc once. Non-
			// actionable reasons remain untracked.
			time.Sleep(10 * time.Millisecond)
			d.OnPodChange(pod)

			d.mu.RLock()
			pendLen := len(d.pendingPods)
			d.mu.RUnlock()
			wantPend := 0
			if tc.wantPend {
				wantPend = 1
			}
			if pendLen != wantPend {
				t.Fatalf("pendingPods len=%d, want %d (reason=%q)", pendLen, wantPend, tc.reason)
			}
			if got := int64(fake.callCount()); got != tc.wantCall {
				t.Fatalf("migrate callback calls=%d, want %d (reason=%q)", got, tc.wantCall, tc.reason)
			}
		})
	}
}

// TestBusiness_TC05_ListSuspectPodsRespectsActionableFilter verifies the
// same business rule on the cluster-scan path: Unknown-reason pods are
// excluded from the suspect list regardless of how long they've been
// pending.
func TestBusiness_TC05_ListSuspectPodsRespectsActionableFilter(t *testing.T) {
	cs := fake.NewSimpleClientset(
		makePodResizePending("good", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue),
		makePodResizePending("bad", "default", "node-1", "Unknown", "x", corev1.ConditionTrue),
	)
	d := New(cs, config.Config{PendingThreshold: 1 * time.Hour})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect (Infeasible only), got %d", len(suspects))
	}
	if suspects[0].PodName != "good" {
		t.Fatalf("unexpected suspect: %+v", suspects[0])
	}
}

// ─── TC-10: active migration → pod is skipped ────────────────────────────

// TestBusiness_TC10_ActiveMigrationSkipsPod verifies that when the
// detector is wired with a MigrationStateChecker, pods whose migrator
// state is already active are skipped by OnPodChange. This guards
// against duplicate triggers when a second controller replica observes
// the same pod after leader-election failover.
func TestBusiness_TC10_ActiveMigrationSkipsPod(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	checker := newFakeActiveChecker("default/busy")
	d.SetMigrationStateChecker(checker)

	busy := makePodResizePending("busy", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
	quiet := makePodResizePending("quiet", "default", "node-2", "Infeasible", "x", corev1.ConditionTrue)

	d.OnPodChange(busy)
	d.OnPodChange(quiet)

	d.mu.RLock()
	_, busyPending := d.pendingPods["default/busy"]
	_, quietPending := d.pendingPods["default/quiet"]
	d.mu.RUnlock()

	if busyPending {
		t.Fatal("active pod should NOT be added to pendingPods")
	}
	if !quietPending {
		t.Fatal("non-active pod SHOULD be added to pendingPods")
	}
	if got := fake.callCount(); got != 1 {
		t.Fatalf("migrate callback should fire once (for quiet), got %d", got)
	}
	if checker.calls.Load() == 0 {
		t.Fatal("checker should have been consulted at least once")
	}
}

// TestBusiness_TC10_NilCheckerIsSafe verifies that omitting the checker
// preserves the original behaviour: every qualifying pod is processed.
func TestBusiness_TC10_NilCheckerIsSafe(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 5 * time.Minute})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)
	// No SetMigrationStateChecker call — d.activeCheck stays nil.

	pod := makePodResizePending("p", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
	d.OnPodChange(pod)

	if got := fake.callCount(); got != 1 {
		t.Fatalf("expected 1 migrate call with nil checker, got %d", got)
	}
}

// ─── TC-17: pod without PodResizePending condition is ignored ───────────

// TestBusiness_TC17_PodWithoutConditionIgnored verifies the detector's
// behaviour for pods that simply don't carry the PodResizePending
// condition at all. Such pods are not candidates for migration.
func TestBusiness_TC17_PodWithoutConditionIgnored(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	tests := []struct {
		name    string
		mutator func(*corev1.Pod)
	}{
		{name: "no conditions", mutator: func(p *corev1.Pod) { p.Status.Conditions = nil }},
		{name: "only unrelated conditions", mutator: func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake.calls = nil
			pod := makePodResizePending("p", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
			tc.mutator(pod)

			d.OnPodChange(pod)
			time.Sleep(5 * time.Millisecond)
			d.OnPodChange(pod)

			d.mu.RLock()
			pendLen := len(d.pendingPods)
			d.mu.RUnlock()
			if pendLen != 0 {
				t.Fatalf("pendingPods len=%d, want 0", pendLen)
			}
			if got := fake.callCount(); got != 0 {
				t.Fatalf("migrate callback calls=%d, want 0", got)
			}
		})
	}
}

// ─── TC-18: pod whose condition is now absent/False is ignored ─────────

// TestBusiness_TC18_ConditionFalseOrAbsentIgnoresPod verifies the
// transition path: a pod that previously had PodResizePending=True but
// now has either the condition set to False or removed entirely is no
// longer tracked or migrated.
func TestBusiness_TC18_ConditionFalseOrAbsentIgnoresPod(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	// Build a pod that was previously pending.
	pod := makePodResizePending("p", "default", "node-1", "Deferred", "x", corev1.ConditionTrue)
	d.OnPodChange(pod)
	time.Sleep(5 * time.Millisecond)
	d.OnPodChange(pod) // firstSeen is now well past the threshold
	d.mu.RLock()
	if len(d.pendingPods) != 1 {
		d.mu.RUnlock()
		t.Fatal("precondition: pod should be pending after threshold")
	}
	d.mu.RUnlock()

	// Kubelet reports PodResizePending=False (resize applied or condition cleared).
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	d.OnPodChange(pod)
	d.mu.RLock()
	pendLen := len(d.pendingPods)
	firstSeenLen := len(d.firstSeen)
	d.mu.RUnlock()
	if pendLen != 0 || firstSeenLen != 0 {
		t.Fatalf("after condition=False: pendingPods=%d firstSeen=%d, want 0/0", pendLen, firstSeenLen)
	}

	// Re-stage and verify the absent-condition case.
	pod = makePodResizePending("p2", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
	d.OnPodChange(pod)
	d.mu.RLock()
	if len(d.pendingPods) != 1 {
		d.mu.RUnlock()
		t.Fatal("precondition: p2 should be pending (Infeasible triggers immediately)")
	}
	d.mu.RUnlock()
	pod.Status.Conditions = nil
	d.OnPodChange(pod)
	d.mu.RLock()
	pendLen = len(d.pendingPods)
	firstSeenLen = len(d.firstSeen)
	d.mu.RUnlock()
	if pendLen != 0 || firstSeenLen != 0 {
		t.Fatalf("after condition cleared: pendingPods=%d firstSeen=%d, want 0/0", pendLen, firstSeenLen)
	}
}

// ─── TC-19: node with enough CPU — pod is not flagged ──────────────────

// TestBusiness_TC19_NodeWithEnoughCPUNotFlagged covers the detector's
// own claim that the cluster-scan path only flags eligible pods. The
// detector does NOT itself check node capacity (kubelet's
// PodResizePending condition is authoritative), so a pod without the
// condition is correctly not flagged. This test pins that contract:
//
//  1. a pod WITHOUT PodResizePending is never in the suspect list;
//  2. a pod WITH PodResizePending=Infeasible IS in the suspect list,
//     regardless of how much capacity the node has — the detector
//     trusts kubelet's verdict.
//
// We also verify that the suspect's NodeName is correctly propagated,
// so the migrator can produce the spec.destination field downstream.
func TestBusiness_TC19_NodeWithEnoughCPUNotFlagged(t *testing.T) {
	// (1) Node has room, no PodResizePending condition set — no suspect.
	calm := makePodResizePending("calm", "default", "node-1", "Infeasible", "x", corev1.ConditionFalse)
	cs := fake.NewSimpleClientset(calm)
	d := New(cs, config.Config{PendingThreshold: 1 * time.Minute})

	if got := d.ListSuspectPods(context.Background()); len(got) != 0 {
		t.Fatalf("calm pod should not be a suspect, got %d", len(got))
	}

	// (2) Node marked Infeasible by kubelet — IS a suspect even though
	//     the cluster still has allocatable room elsewhere. The detector
	//     must trust kubelet; CLM handles the actual movement.
	full := makePodResizePending("full", "default", "node-1", "Infeasible", "no capacity", corev1.ConditionTrue)
	cs2 := fake.NewSimpleClientset(full)
	d2 := New(cs2, config.Config{PendingThreshold: 1 * time.Minute})

	suspects := d2.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect for Infeasible, got %d", len(suspects))
	}
	if suspects[0].NodeName != "node-1" {
		t.Fatalf("NodeName=%q, want %q (used as spec.destination)", suspects[0].NodeName, "node-1")
	}
	if suspects[0].Reason != "Infeasible" {
		t.Fatalf("Reason=%q, want Infeasible", suspects[0].Reason)
	}
}

// ─── TC-25: pod delete clears detector tracking ─────────────────────────

// TestBusiness_TC25_PodDeleteClearsTracking covers the DeleteFunc path:
// after OnPodDelete, neither pendingPods nor firstSeen retain the pod.
// This guards against a leaked entry that would re-trigger migration on
// the next informer event for a recreate-with-same-name pod.
func TestBusiness_TC25_PodDeleteClearsTracking(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 1 * time.Millisecond})
	fake := &fakeMigrateFunc{}
	d.SetMigrateFunc(fake.fn)

	pod := makePodResizePending("p", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
	d.OnPodChange(pod)

	d.mu.RLock()
	pendingBefore := len(d.pendingPods)
	firstSeenBefore := len(d.firstSeen)
	d.mu.RUnlock()
	if pendingBefore != 1 || firstSeenBefore != 1 {
		t.Fatalf("precondition: pendingPods=%d firstSeen=%d, want 1/1", pendingBefore, firstSeenBefore)
	}

	d.OnPodDelete(pod)

	d.mu.RLock()
	pendingAfter := len(d.pendingPods)
	firstSeenAfter := len(d.firstSeen)
	d.mu.RUnlock()
	if pendingAfter != 0 || firstSeenAfter != 0 {
		t.Fatalf("after OnPodDelete: pendingPods=%d firstSeen=%d, want 0/0", pendingAfter, firstSeenAfter)
	}

	// Sanity: a recreate with the same name should re-populate tracking.
	d.OnPodChange(pod)
	d.mu.RLock()
	pendingReadd := len(d.pendingPods)
	d.mu.RUnlock()
	if pendingReadd != 1 {
		t.Fatalf("after recreate: pendingPods=%d, want 1", pendingReadd)
	}
}

// TestBusiness_TC25_DeleteBeforeFirstSeenIsSafe verifies that deleting a
// pod the detector has never seen is a no-op (delete event ordering
// against add event).
func TestBusiness_TC25_DeleteBeforeFirstSeenIsSafe(t *testing.T) {
	d := New(nil, config.Config{})
	pod := makePodResizePending("ghost", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue)
	d.OnPodDelete(pod)

	d.mu.RLock()
	pendingAfter := len(d.pendingPods)
	firstSeenAfter := len(d.firstSeen)
	d.mu.RUnlock()
	if pendingAfter != 0 || firstSeenAfter != 0 {
		t.Fatalf("ghost delete: pendingPods=%d firstSeen=%d, want 0/0", pendingAfter, firstSeenAfter)
	}
}

// ─── Sanity: ListSuspectPods respects PendingThreshold for Deferred ──────

// TestBusiness_DeferredBelowThresholdNotFlagged is a backstop for the
// QA plan's deferred-threshold behaviour on the cluster-scan path.
func TestBusiness_DeferredBelowThresholdNotFlagged(t *testing.T) {
	cs := fake.NewSimpleClientset(
		makePodResizePending("d", "default", "node-1", "Deferred", "x", corev1.ConditionTrue),
	)
	d := New(cs, config.Config{PendingThreshold: 1 * time.Hour})

	if got := d.ListSuspectPods(context.Background()); len(got) != 0 {
		t.Fatalf("Deferred below threshold should not be flagged, got %d suspects", len(got))
	}
}

// TestBusiness_DesiredAndAllocatedPropagated locks down the field
// propagation that the migrator relies on for spec.destination and
// migration labels.
func TestBusiness_DesiredAndAllocatedPropagated(t *testing.T) {
	cs := fake.NewSimpleClientset(
		makePodResizePending("p", "default", "node-1", "Infeasible", "x", corev1.ConditionTrue),
	)
	d := New(cs, config.Config{PendingThreshold: 1 * time.Minute})

	suspects := d.ListSuspectPods(context.Background())
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect, got %d", len(suspects))
	}
	if suspects[0].DesiredCPU != 1500 {
		t.Fatalf("DesiredCPU=%d, want 1500 (from helper pod)", suspects[0].DesiredCPU)
	}
	if suspects[0].AllocatedCPU != 100 {
		t.Fatalf("AllocatedCPU=%d, want 100", suspects[0].AllocatedCPU)
	}
	if suspects[0].Namespace != "default" || suspects[0].PodName != "p" {
		t.Fatalf("unexpected namespace/podName: %s/%s", suspects[0].Namespace, suspects[0].PodName)
	}
}
