package detector

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"castai-workload-resize-migrator/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	woopEnabledLabel       = "workload-autoscaler.cast.ai/enabled"
	migrationEnabledLabel = "live.cast.ai/migration-enabled"
)

type PodPendingInfo struct {
	Namespace    string
	PodName      string
	WorkloadName string
	WorkloadKind string
	NodeName     string
	AllocatedCPU int64 // milli-cores
	DesiredCPU   int64 // milli-cores
	PendingSince time.Time
}

type NodeInfo struct {
	Name           string
	AllocatableCPU int64 // milli-cores
	Pods           map[string]*PodPendingInfo
}

type Detector struct {
	clientset kubernetes.Interface
	cfg       config.Config

	mu          sync.RWMutex
	pendingPods map[string]*PodPendingInfo
	nodes       map[string]*NodeInfo
	nodePodSum  map[string]int64 // node -> sum of requested CPU of all pods on node
	firstSeen   map[string]time.Time
}

func New(clientset kubernetes.Interface, cfg config.Config) *Detector {
	return &Detector{
		clientset:   clientset,
		cfg:         cfg,
		pendingPods: make(map[string]*PodPendingInfo),
		nodes:       make(map[string]*NodeInfo),
		nodePodSum:  make(map[string]int64),
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
	// This prevents triggering migrations for capacity pods, system pods,
	// and workloads that CLM cannot migrate.
	if pod.Labels == nil || pod.Labels[migrationEnabledLabel] != "true" {
		return
	}

	key := pod.Namespace + "/" + pod.Name

	desired, allocated, pending := d.extractResizeStatus(pod)
	slog.Debug("OnPodChange", "key", key, "node", pod.Spec.NodeName, "desired", desired, "allocated", allocated, "pending", pending)
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

	// Wait for threshold before considering it a real signal.
	if time.Since(pendingSince) < d.cfg.PendingThreshold {
		return
	}

	workloadName, workloadKind := d.resolveWorkload(pod)

	info := &PodPendingInfo{
		Namespace:    pod.Namespace,
		PodName:      pod.Name,
		WorkloadName: workloadName,
		WorkloadKind: workloadKind,
		NodeName:     pod.Spec.NodeName,
		AllocatedCPU: allocated,
		DesiredCPU:   desired,
		PendingSince: pendingSince,
	}
	d.pendingPods[key] = info
	slog.Info("detected pending upsize", "pod", key, "allocated", allocated, "desired", desired, "pendingSince", pendingSince)
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
	if node == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	cpu := node.Status.Allocatable.Cpu().MilliValue()
	d.nodes[node.Name] = &NodeInfo{
		Name:           node.Name,
		AllocatableCPU: cpu,
		Pods:           make(map[string]*PodPendingInfo),
	}
}

func (d *Detector) OnNodeDelete(node *corev1.Node) {
	if node == nil {
		return
	}
	d.mu.Lock()
	delete(d.nodes, node.Name)
	delete(d.nodePodSum, node.Name)
	d.mu.Unlock()
}

// ListSuspectPods returns pending pods whose node cannot fit the pending CPU delta.
func (d *Detector) ListSuspectPods(ctx context.Context) []*PodPendingInfo {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Aggregate pending pods by node.
	for _, info := range d.pendingPods {
		if info.NodeName == "" {
			continue
		}
		n, ok := d.nodes[info.NodeName]
		if !ok {
			continue
		}
		if n.Pods == nil {
			n.Pods = make(map[string]*PodPendingInfo)
		}
		n.Pods[info.Namespace+"/"+info.PodName] = info
	}

	var suspects []*PodPendingInfo
	for name, n := range d.nodes {
		if len(n.Pods) == 0 {
			continue
		}
		var totalPending int64
		for _, p := range n.Pods {
			delta := p.DesiredCPU - p.AllocatedCPU
			if delta > 0 {
				totalPending += delta
			}
		}
		if n.AllocatableCPU == 0 {
			continue
		}
		ratio := float64(totalPending) / float64(n.AllocatableCPU)
		if ratio < d.cfg.NodeDeltaThreshold {
			slog.Debug("pending delta below threshold", "node", name, "delta", totalPending, "ratio", ratio)
			continue
		}

		// Real contention check: node must not have room for the pending delta.
		used := d.nodePodSum[name]
		available := n.AllocatableCPU - used
		if available >= totalPending {
			slog.Debug("node has enough available CPU", "node", name, "available", available, "pendingDelta", totalPending)
			continue
		}

		slog.Info("suspect node detected", "node", name, "pendingDelta", totalPending, "allocatable", n.AllocatableCPU, "used", used, "ratio", ratio)
		for _, p := range n.Pods {
			suspects = append(suspects, p)
		}
	}
	return suspects
}

// RefreshNodePodSum recalculates the sum of requested CPU per node from the API server.
func (d *Detector) RefreshNodePodSum(ctx context.Context) error {
	pods, err := d.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	sums := make(map[string]int64)
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		var sum int64
		for _, c := range pod.Spec.Containers {
			if c.Resources.Requests.Cpu() != nil {
				sum += c.Resources.Requests.Cpu().MilliValue()
			}
		}
		sums[pod.Spec.NodeName] += sum
	}

	d.mu.Lock()
	d.nodePodSum = sums
	d.mu.Unlock()
	return nil
}

func (d *Detector) extractResizeStatus(pod *corev1.Pod) (desired, allocated int64, pending bool) {
	// Kubernetes in-place resize:
	// - desired/requested resources stay in spec.containers[].resources.requests
	// - already-allocated/applied resources are in containerStatus.allocatedResources
	// - containerStatus.resources.requests reflects the currently effective resources
	//   (same as allocatedResources until the resize is applied).
	// Therefore, to detect a pending resize, compare spec requests with allocatedResources.
	for _, c := range pod.Spec.Containers {
		specCPU := c.Resources.Requests.Cpu().MilliValue()
		var allocatedCPU int64
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == c.Name {
				if cs.AllocatedResources != nil {
					allocatedCPU = cs.AllocatedResources.Cpu().MilliValue()
				} else if cs.Resources != nil && cs.Resources.Requests.Cpu() != nil {
					allocatedCPU = cs.Resources.Requests.Cpu().MilliValue()
				}
				break
			}
		}
		if specCPU > allocatedCPU {
			desired += specCPU
			allocated += allocatedCPU
		}
	}
	pending = desired > allocated
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

func parseMilliCPU(s string) int64 {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.MilliValue()
}

func parseCPUInt(s string) int64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(v * 1000)
}

// GetPod returns a pod by namespace/name (used by safety scan).
func (d *Detector) GetPod(ctx context.Context, ns, name string) (*corev1.Pod, error) {
	return d.clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
}

func (d *Detector) AnnotationKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}
