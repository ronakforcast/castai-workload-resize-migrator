package detector

import (
	"context"
	"testing"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nodeWithTemplate builds a node carrying the CAST AI node-template label.
func nodeWithTemplate(name, tmpl string) *corev1.Node {
	labels := map[string]string{}
	if tmpl != "" {
		labels["scheduling.cast.ai/node-template"] = tmpl
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
	}
}

// scopedMigrationLabels returns labels marking a pod as CLM-eligible.
func scopedMigrationLabels() map[string]string {
	return map[string]string{"live.cast.ai/migration-enabled": "true"}
}

// fakeMigrateRecorder counts migrate callback invocations.
type fakeMigrateRecorder struct {
	pods []*PodPendingInfo
}

func (f *fakeMigrateRecorder) record(_ context.Context, p *PodPendingInfo) error {
	f.pods = append(f.pods, p)
	return nil
}

// TestPodInScopeEmptyConfigAllowsAll verifies that an empty
// SourceNodeTemplates config keeps the controller unscoped (backward
// compatible): pods on any node, including unknown or unlabeled nodes,
// remain in scope.
func TestPodInScopeEmptyConfigAllowsAll(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: nil})
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if !d.podInScope(pod) {
		t.Fatal("expected pod to be in scope with empty SourceNodeTemplates")
	}
}

// TestPodInScopeMatchingTemplate verifies a pod on a node whose
// node-template label matches one of the configured templates is in
// scope.
func TestPodInScopeMatchingTemplate(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template", "other-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "clm-template"))
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if !d.podInScope(pod) {
		t.Fatal("expected pod on clm-template node to be in scope")
	}
}

// TestPodInScopeNonMatchingTemplate verifies a pod on a node from a
// different template is skipped.
func TestPodInScopeNonMatchingTemplate(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "default-by-castai"))
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if d.podInScope(pod) {
		t.Fatal("expected pod on default-by-castai node to be out of scope")
	}
}

// TestPodInScopeUnlabeledNode verifies a node without the template label
// is treated as out of scope when scoping is configured.
func TestPodInScopeUnlabeledNode(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", ""))
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if d.podInScope(pod) {
		t.Fatal("expected pod on unlabeled node to be out of scope")
	}
}

// TestPodInScopeUnknownNode verifies a pod on a node the informer has
// not yet seen is skipped (conservative) while scoping is configured.
func TestPodInScopeUnknownNode(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if d.podInScope(pod) {
		t.Fatal("expected pod on unknown node to be out of scope")
	}
}

// TestOnPodChangeScopedSkipsNonMatchingNode verifies the event-driven
// path: with scoping configured, a pending pod on a non-scoped node
// never triggers the migrate callback.
func TestOnPodChangeScopedSkipsNonMatchingNode(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "default-by-castai"))

	rec := &fakeMigrateRecorder{}
	d.SetMigrateFunc(rec.record)

	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	d.OnPodChange(pod)

	if len(rec.pods) != 0 {
		t.Fatalf("expected no migrate callback for out-of-scope pod, got %d", len(rec.pods))
	}
}

// TestOnPodChangeScopedAllowsMatchingNode verifies the event-driven
// path: with scoping configured, a pending pod on a scoped node triggers
// the migrate callback.
func TestOnPodChangeScopedAllowsMatchingNode(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "clm-template"))

	rec := &fakeMigrateRecorder{}
	d.SetMigrateFunc(rec.record)

	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	d.OnPodChange(pod)

	if len(rec.pods) != 1 {
		t.Fatalf("expected 1 migrate callback for in-scope pod, got %d", len(rec.pods))
	}
}

// TestOnNodeDeleteRemovesTemplate verifies deleted nodes free their
// template record so their pods fall back to out-of-scope.
func TestOnNodeDeleteRemovesTemplate(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "clm-template"))
	d.OnNodeDelete(nodeWithTemplate("node-1", "clm-template"))

	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	if d.podInScope(pod) {
		t.Fatal("expected pod to be out of scope after node delete")
	}
}

// TestListSuspectPodsScoped verifies the safety scan applies the same
// node-template scoping as the event path.
func TestListSuspectPodsScoped(t *testing.T) {
	ctx := context.Background()
	d := New(nil, config.Config{PendingThreshold: 0, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "clm-template"))

	// In-memory fallback path (no clientset wired): the pendingPods map
	// only contains what OnPodChange admitted, which respects scoping.
	pod := podWithResizePending("Infeasible", "no capacity", scopedMigrationLabels())
	d.OnPodChange(pod)

	suspects := d.ListSuspectPods(ctx)
	if len(suspects) != 1 {
		t.Fatalf("expected 1 suspect, got %d", len(suspects))
	}
	if suspects[0].NodeName != "node-1" {
		t.Fatalf("unexpected node: %s", suspects[0].NodeName)
	}
}

// TestScopedDeferredStillHonorsThreshold is a smoke check that scoping
// does not disturb the Deferred threshold logic for in-scope pods.
func TestScopedDeferredStillHonorsThreshold(t *testing.T) {
	d := New(nil, config.Config{PendingThreshold: time.Hour, SourceNodeTemplates: []string{"clm-template"}})
	d.OnNodeChange(nodeWithTemplate("node-1", "clm-template"))

	rec := &fakeMigrateRecorder{}
	d.SetMigrateFunc(rec.record)

	pod := podWithResizePending("Deferred", "node full", scopedMigrationLabels())
	d.OnPodChange(pod)

	if len(rec.pods) != 0 {
		t.Fatalf("expected Deferred below threshold to not trigger, got %d callbacks", len(rec.pods))
	}
}
