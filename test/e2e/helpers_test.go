//go:build e2e

// Package e2e contains end-to-end tests for the castai-workload-resize-migrator.
//
// These tests run against a live Kubernetes cluster with CAST AI CLM enabled.
// The controller must be running (locally or in-cluster) during the tests.
//
// Run with:
//
//	go test -tags=e2e ./test/e2e/... -kubeconfig ~/.kube/config -v -timeout 20m
package e2e

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfigPath string
	clientset     *kubernetes.Clientset
	dynClient     dynamic.Interface
)

const (
	clmNodeSelector      = "scheduling.cast.ai/node-template: clm-live-migration-template"
	migrationGroup       = "live.cast.ai"
	migrationVersion     = "v1"
	migrationResource    = "migrations"
	testImage            = "nginx:latest"
	curlImage             = "curlimages/curl:latest"
	fillImage             = "busybox:latest"
	defaultWaitTimeout    = 5 * time.Minute
	pollInterval         = 5 * time.Second
)

var migrationGVR = schema.GroupVersionResource{
	Group:    migrationGroup,
	Version:  migrationVersion,
	Resource: migrationResource,
}

func TestMain(m *testing.M) {
	flag.StringVar(&kubeconfigPath, "kubeconfig", os.Getenv("HOME")+"/.kube/config", "path to kubeconfig")
	flag.Parse()

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build kubeconfig: %v\n", err)
		os.Exit(1)
	}

	clientset, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create clientset: %v\n", err)
		os.Exit(1)
	}

	dynClient, err = dynamic.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create dynamic client: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// ─── Helpers ───

func createNamespace(t *testing.T, name string) {
	t.Helper()
	_, err := clientset.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

func deleteNamespace(t *testing.T, name string) {
	t.Helper()
	_ = clientset.CoreV1().Namespaces().Delete(context.Background(), name, metav1.DeleteOptions{})
}

func createResizeRBAC(t *testing.T, ns string) {
	t.Helper()
	ctx := context.Background()

	_, err := clientset.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "resize-patcher"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods", "pods/resize"}, Verbs: []string{"get", "list", "patch"}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}

	_, err = clientset.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "resize-patcher"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "default", Namespace: ns}},
		RoleRef:    rbacv1.RoleRef{Kind: "Role", Name: "resize-patcher", APIGroup: "rbac.authorization.k8s.io"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create rolebinding: %v", err)
	}
}

func deployNginx(t *testing.T, ns, deployName string, replicas int32, cpu string) {
	t.Helper()
	ctx := context.Background()

	_, err := clientset.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deployName},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": deployName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": deployName}},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"scheduling.cast.ai/node-template": "clm-live-migration-template"},
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: testImage,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(cpu),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
							ResizePolicy: []corev1.ContainerResizePolicy{
								{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create deployment %s: %v", deployName, err)
	}
}

func deployFiller(t *testing.T, ns, name, cpu string) {
	t.Helper()
	ctx := context.Background()
	replicas := int32(1)

	_, err := clientset.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"scheduling.cast.ai/node-template": "clm-live-migration-template"},
					Containers: []corev1.Container{
						{
							Name:  "stress",
							Image: fillImage,
							Command: []string{"sh", "-c", "while true; do true; done"},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(cpu),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create filler %s: %v", name, err)
	}
}

func waitForDeployment(t *testing.T, ns, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("deployment %s/%s not ready after %v", ns, name, timeout)
		case <-time.Tick(pollInterval):
			dep, err := clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				continue
			}
			if dep.Status.ReadyReplicas == *dep.Spec.Replicas && *dep.Spec.Replicas > 0 {
				return
			}
		}
	}
}

func getPodName(t *testing.T, ns, labelKey, labelVal string) string {
	t.Helper()
	pods, err := clientset.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", labelKey, labelVal),
	})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("no pods found with %s=%s in %s", labelKey, labelVal, ns)
	}
	return pods.Items[0].Name
}

func patchPodResize(t *testing.T, ns, podName, containerName, cpu string) {
	t.Helper()
	ctx := context.Background()

	// Must include memory in the patch — Kubernetes rejects patches that
	// omit existing resource requests (treats omission as removal).
	patchJSON := fmt.Sprintf(
		`{"spec":{"containers":[{"name":"%s","resources":{"requests":{"cpu":"%s","memory":"128Mi"}}}]}}`,
		containerName, cpu,
	)

	// Use the REST client directly — the typed Patch() method doesn't
	// correctly handle the /resize subresource content type.
	result := clientset.CoreV1().RESTClient().
		Patch(types.StrategicMergePatchType).
		Resource("pods").
		Namespace(ns).
		Name(podName).
		SubResource("resize").
		Body([]byte(patchJSON)).
		Do(ctx)
	if result.Error() != nil {
		t.Fatalf("failed to patch pod %s/%s resize: %v", ns, podName, result.Error())
	}

	// Verify the patch was applied.
	pod, err := clientset.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod after patch: %v", err)
	}
	specCPU := pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue()
	expectedCPU := resource.MustParse(cpu)
	t.Logf("patched pod %s/%s CPU to %s via /resize (spec now: %dm)", ns, podName, cpu, specCPU)

	if specCPU != expectedCPU.MilliValue() {
		t.Fatalf("patch did not apply: expected spec CPU=%s (%dm), got %dm", cpu, expectedCPU.MilliValue(), specCPU)
	}
}

func waitPodCompleted(t *testing.T, ns, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.Tick(2 * time.Second):
			pod, err := clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				continue
			}
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				return
			}
		}
	}
}

func getPodResizePending(t *testing.T, ns, podName string) (bool, string, string) {
	t.Helper()
	pod, err := clientset.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s/%s: %v", ns, podName, err)
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodResizePending && cond.Status == corev1.ConditionTrue {
			return true, cond.Reason, cond.Message
		}
	}
	return false, "", ""
}

func waitForPodResizePending(t *testing.T, ns, podName string, timeout time.Duration) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("pod %s/%s did not get PodResizePending=True after %v", ns, podName, timeout)
		case <-time.Tick(pollInterval):
			pending, reason, msg := getPodResizePending(t, ns, podName)
			if pending {
				return reason, msg
			}
		}
	}
}

func getPodAllocatedCPU(t *testing.T, ns, podName string) string {
	t.Helper()
	pod, err := clientset.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s/%s: %v", ns, podName, err)
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return ""
	}
	cs := pod.Status.ContainerStatuses[0]
	if cs.AllocatedResources != nil {
		return cs.AllocatedResources.Cpu().String()
	}
	return ""
}

func migrationExistsForPod(ns, podName string) bool {
	migrations, err := dynClient.Resource(migrationGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return false
	}
	for _, item := range migrations.Items {
		specPod, _, _ := unstructured.NestedString(item.Object, "spec", "podName")
		if specPod == podName {
			return true
		}
	}
	return false
}

func waitForMigrationForPod(t *testing.T, ns, podName string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("no migration created for pod %s/%s after %v", ns, podName, timeout)
		case <-time.Tick(pollInterval):
			if migrationExistsForPod(ns, podName) {
				return
			}
		}
	}
}

func waitForNoMigrationForPod(t *testing.T, ns, podName string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return // no migration found within timeout — pass
		case <-time.Tick(pollInterval):
			if migrationExistsForPod(ns, podName) {
				t.Fatalf("unexpected migration created for pod %s/%s", ns, podName)
			}
		}
	}
}

func waitForMigrationState(t *testing.T, ns, podName, expectedState string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("migration for pod %s/%s did not reach %s after %v", ns, podName, expectedState, timeout)
		case <-time.Tick(10 * time.Second):
			migrations, err := dynClient.Resource(migrationGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for _, item := range migrations.Items {
				specPod, _, _ := unstructured.NestedString(item.Object, "spec", "podName")
				if specPod != podName {
					continue
				}
				state, found, _ := unstructured.NestedString(item.Object, "status", "state")
				if found && state == expectedState {
					return
				}
			}
		}
	}
}

func deleteAllMigrations(t *testing.T, ns string) {
	t.Helper()
	_ = dynClient.Resource(migrationGVR).Namespace(ns).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{})
}
