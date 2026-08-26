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
	AllocatedCPU int64 // milli-cores
	DesiredCPU   int64 // milli-cores
	Reason       string // Deferred or Infeasible
	Message      string // kubelet's explanation
	PendingSince time.Time
}

type Detector struct {
	clientset kubernetes.Interface
	cfg       config.Config

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

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.firstSeen[key]; !ok {
		d.firstSeen[key] = time.Now()
	}
	pendingSince := d.firstSeen[key]

	// For Infeasible resizes, the pod can never fit on this node.
	// Skip the threshold wait and trigger immediately.
	if reason == reasonInfeasible {
		// Even for Infeasible, we still record firstSeen and add to pending.
		// The migrator will pick it up on the next safety scan.
		// But we don't wait for the threshold — add it right away.
	} else {
		// For Deferred resizes, wait for the threshold before considering
		// it a real signal. The node might free up on its own.
		if time.Since(pendingSince) < d.cfg.PendingThreshold {
			return
		}
	}

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
	d.pendingPods[key] = info
	slog.Info("detected pending upsize", "pod", key, "allocated", allocated, "desired", desired, "reason", reason, "message", message, "pendingSince", pendingSince)
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

// ListSuspectPods returns all pending pods that are ready for migration.
// A pod is ready if:
//   - It has PodResizePending=True with reason Infeasible (always ready)
//   - It has PodResizePending=True with reason Deferred AND has been pending
//     for longer than PendingThreshold
func (d *Detector) ListSuspectPods(ctx context.Context) []*PodPendingInfo {
	d.mu.Lock()
	defer d.mu.Unlock()

	var suspects []*PodPendingInfo
	for key, info := range d.pendingPods {
		if info.Reason == reasonInfeasible {
			// Infeasible pods are always candidates — they can never resize on this node.
			suspects = append(suspects, info)
			continue
		}

		// For Deferred pods, check if the threshold has been crossed.
		pendingSince := d.firstSeen[key]
		if time.Since(pendingSince) >= d.cfg.PendingThreshold {
			suspects = append(suspects, info)
		}
	}
	return suspects
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
