package detector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	migrationEnabledLabel = "live.cast.ai/migration-enabled"
	podResizePendingType  = "PodResizePending"
	reasonDeferred        = "Deferred"
	reasonInfeasible      = "Infeasible"
)

type PodPendingInfo struct {
	Namespace    string
	PodName      string
	WorkloadName string
	WorkloadKind string
	NodeName     string
	AllocatedCPU int64  // milli-cores
	DesiredCPU   int64  // milli-cores
	Reason       string // Deferred or Infeasible
	Message      string // kubelet's explanation
	PendingSince time.Time
}

// MigrateFunc is called immediately when a pod qualifies for migration.
// The detector calls this from OnPodChange — no safety scan delay.
type MigrateFunc func(ctx context.Context, p *PodPendingInfo) error

// MigrationStateChecker reports whether the migrator already has an
// active migration for a given pod. The detector consults this in
// OnPodChange so that pods whose migrations are already in-flight on
// another replica (or about to retry) are not double-triggered.
//
// It is nil-safe: when no checker is wired up the detector behaves as
// it always has (no de-duplication against the migrator).
type MigrationStateChecker interface {
	IsActive(namespace, podName string) bool
}

type Detector struct {
	clientset   kubernetes.Interface
	cfg         config.Config
	migrateFunc MigrateFunc
	activeCheck MigrationStateChecker

	mu          sync.RWMutex
	pendingPods map[string]*PodPendingInfo
	firstSeen   map[string]time.Time
}

func New(clientset kubernetes.Interface, cfg config.Config) *Detector {
	return &Detector{
		clientset:   clientset,
		cfg:         cfg,
		pendingPods: make(map[string]*PodPendingInfo),
		firstSeen:   make(map[string]time.Time),
	}
}

// SetMigrateFunc sets the callback used to create migrations immediately.
func (d *Detector) SetMigrateFunc(fn MigrateFunc) {
	d.migrateFunc = fn
}

// SetMigrationStateChecker installs a callback that reports whether the
// migrator already has an active migration for a pod. When set, the
// detector will skip pods for which the checker returns true, avoiding
// duplicate triggers. Calling with nil disables the check.
func (d *Detector) SetMigrationStateChecker(c MigrationStateChecker) {
	d.activeCheck = c
}

func (d *Detector) Run(ctx context.Context) {
	<-ctx.Done()
}

func (d *Detector) OnPodChange(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	// Only process pods that CLM has labeled as migration-eligible.
	if pod.Labels == nil || pod.Labels[migrationEnabledLabel] != "true" {
		return
	}

	key := pod.Namespace + "/" + pod.Name

	reason, message, pending := d.extractResizeStatus(pod)
	slog.Debug("OnPodChange", "key", key, "node", pod.Spec.NodeName, "pending", pending, "reason", reason)

	if !pending {
		d.mu.Lock()
		delete(d.firstSeen, key)
		delete(d.pendingPods, key)
		d.mu.Unlock()
		return
	}

	// If the migrator already has an active migration for this pod
	// (e.g. observed on another replica via leader-election failover),
	// skip it. This is best-effort: the migrator is also idempotent
	// against duplicate triggers, but checking here avoids emitting
	// spurious log lines and re-populating the in-memory pending map.
	if d.activeCheck != nil && d.activeCheck.IsActive(pod.Namespace, pod.Name) {
		slog.Debug("pod already has an active migration; skipping", "key", key)
		return
	}

	d.mu.Lock()
	if _, ok := d.firstSeen[key]; !ok {
		d.firstSeen[key] = time.Now()
	}
	pendingSince := d.firstSeen[key]
	d.mu.Unlock()

	// Determine if this pod qualifies for immediate migration.
	eligible := false
	if !d.isActionableReason(reason) {
		// Unknown / unrecognized reasons (including missing/empty) are
		// not actionable — only Infeasible and Deferred trigger the
		// migration path. Tracking the firstSeen timestamp is harmless
		// because the next informer update may carry an actionable
		// reason; until then we do nothing.
	} else if reason == reasonInfeasible {
		// Infeasible: node can never fit the resize. Trigger immediately.
		eligible = true
	} else {
		// Deferred: wait for threshold. Kubelet retries periodically,
		// so the informer will fire again. Check if threshold has passed.
		if time.Since(pendingSince) >= d.cfg.PendingThreshold {
			eligible = true
		}
	}

	if !eligible {
		slog.Debug("pending but not yet eligible", "pod", key, "reason", reason, "pendingFor", time.Since(pendingSince), "threshold", d.cfg.PendingThreshold)
		return
	}

	// Pod qualifies — add to pending and trigger migration immediately.
	desired, allocated := d.extractCPUValues(pod)
	workloadName, workloadKind := d.resolveWorkload(pod)

	info := &PodPendingInfo{
		Namespace:    pod.Namespace,
		PodName:      pod.Name,
		WorkloadName: workloadName,
		WorkloadKind: workloadKind,
		NodeName:     pod.Spec.NodeName,
		AllocatedCPU: allocated,
		DesiredCPU:   desired,
		Reason:       reason,
		Message:      message,
		PendingSince: pendingSince,
	}

	d.mu.Lock()
	d.pendingPods[key] = info
	d.mu.Unlock()

	slog.Info("detected pending upsize", "pod", key, "allocated", allocated, "desired", desired, "reason", reason, "message", message, "pendingSince", pendingSince)

	// Trigger migration immediately via callback.
	if d.migrateFunc != nil {
		if err := d.migrateFunc(context.Background(), info); err != nil {
			slog.Error("failed to trigger migration", "pod", key, "error", err)
		}
	}
}

func (d *Detector) OnPodDelete(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	key := pod.Namespace + "/" + pod.Name
	d.mu.Lock()
	delete(d.firstSeen, key)
	delete(d.pendingPods, key)
	d.mu.Unlock()
}

func (d *Detector) OnNodeChange(node *corev1.Node) {
	// Node tracking is no longer needed — kubelet's PodResizePending
	// condition is authoritative for whether the node can fit the resize.
}

func (d *Detector) OnNodeDelete(node *corev1.Node) {
	// No-op — we don't track nodes anymore.
}

// ListSuspectPods scans the cluster for pods that qualify for migration
// but were missed by the event-driven OnPodChange path (e.g., controller
// restart, brief informer cache gap). It is a fallback to the in-memory
// pendingPods map.
//
// The scan lists pods via the provided kubernetes.Interface and applies
// the same candidate filter as OnPodChange:
//   - label live.cast.ai/migration-enabled=true
//   - PodResizePending=True
//   - reason Infeasible (always eligible) or Deferred beyond
//     PendingThreshold.
//
// PendingSince is taken from the in-memory firstSeen map when available,
// otherwise now is used (so Deferred pods that were never seen via the
// informer will not pass the threshold on a single scan).
func (d *Detector) ListSuspectPods(ctx context.Context) []*PodPendingInfo {
	// Fallback when no clientset is wired up (e.g., unit tests that
	// exercise only the in-memory map).
	if d.clientset == nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		suspects := make([]*PodPendingInfo, 0, len(d.pendingPods))
		for _, info := range d.pendingPods {
			suspects = append(suspects, info)
		}
		return suspects
	}

	// Use label and field selectors to limit the safety scan to pods
	// the controller actually cares about. This avoids paginating every
	// pod in the cluster and reduces API server load.
	list, err := d.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		LabelSelector: migrationEnabledLabel + "=true",
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		slog.Error("safety scan: failed to list pods", "error", err)
		// On API error, fall back to whatever is in memory so the
		// caller's retry loop still surfaces something useful.
		d.mu.Lock()
		defer d.mu.Unlock()
		suspects := make([]*PodPendingInfo, 0, len(d.pendingPods))
		for _, info := range d.pendingPods {
			suspects = append(suspects, info)
		}
		return suspects
	}

	now := time.Now()
	suspects := make([]*PodPendingInfo, 0, len(list.Items))

	for i := range list.Items {
		pod := &list.Items[i]

		if pod.Labels == nil || pod.Labels[migrationEnabledLabel] != "true" {
			continue
		}

		reason, message, pending := d.extractResizeStatus(pod)
		if !pending {
			continue
		}

		key := pod.Namespace + "/" + pod.Name

		d.mu.RLock()
		firstSeen, seen := d.firstSeen[key]
		d.mu.RUnlock()

		pendingSince := firstSeen
		if !seen {
			pendingSince = now
		}

		eligible := false
		switch reason {
		case reasonInfeasible:
			eligible = true
		case reasonDeferred:
			if now.Sub(pendingSince) >= d.cfg.PendingThreshold {
				eligible = true
			}
		}

		if !eligible {
			continue
		}

		desired, allocated := d.extractCPUValues(pod)
		workloadName, workloadKind := d.resolveWorkload(pod)

		suspects = append(suspects, &PodPendingInfo{
			Namespace:    pod.Namespace,
			PodName:      pod.Name,
			WorkloadName: workloadName,
			WorkloadKind: workloadKind,
			NodeName:     pod.Spec.NodeName,
			AllocatedCPU: allocated,
			DesiredCPU:   desired,
			Reason:       reason,
			Message:      message,
			PendingSince: pendingSince,
		})
	}

	return suspects
}

// isActionableReason reports whether the given PodResizePending reason
// should drive a migration. Only Infeasible and Deferred are actionable;
// unknown or missing reasons are ignored. Extracted as a method so the
// OnPodChange and ListSuspectPods paths share the same gating policy.
func (d *Detector) isActionableReason(reason string) bool {
	return reason == reasonInfeasible || reason == reasonDeferred
}

// extractResizeStatus checks the pod's PodResizePending condition.
// Returns (reason, message, pending).
func (d *Detector) extractResizeStatus(pod *corev1.Pod) (reason, message string, pending bool) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == podResizePendingType && cond.Status == corev1.ConditionTrue {
			return cond.Reason, cond.Message, true
		}
	}
	return "", "", false
}

// extractCPUValues returns the desired and allocated CPU from the pod's
// spec and container statuses. Used for logging and PodPendingInfo.
func (d *Detector) extractCPUValues(pod *corev1.Pod) (desired, allocated int64) {
	for _, c := range pod.Spec.Containers {
		specCPU := c.Resources.Requests.Cpu().MilliValue()
		desired += specCPU

		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == c.Name {
				if cs.AllocatedResources != nil {
					allocated += cs.AllocatedResources.Cpu().MilliValue()
				} else if cs.Resources != nil && cs.Resources.Requests.Cpu() != nil {
					allocated += cs.Resources.Requests.Cpu().MilliValue()
				}
				break
			}
		}
	}
	return
}

func (d *Detector) resolveWorkload(pod *corev1.Pod) (name, kind string) {
	for _, owner := range pod.OwnerReferences {
		switch owner.Kind {
		case "ReplicaSet":
			name = strings.TrimSuffix(owner.Name, "-"+pod.Labels["pod-template-hash"])
			kind = "Deployment"
			return
		case "StatefulSet", "DaemonSet", "Job", "ReplicationController":
			name = owner.Name
			kind = owner.Kind
			return
		}
	}
	name = pod.Name
	kind = "Pod"
	return
}

func (d *Detector) AnnotationKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}
