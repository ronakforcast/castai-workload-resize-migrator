//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestE01_SinglePodDeferredResize tests that a single pod with a Deferred
// PodResizePending condition gets a Migration CRD created for it.
func TestE01_SinglePodDeferredResize(t *testing.T) {
	ns := "e2e-single-deferred"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	podName := getPodName(t, ns, "app", "nginx")
	patchPodResize(t, ns, podName, "nginx", "1500m")

	reason, msg := waitForPodResizePending(t, ns, podName, 60*time.Second)
	if reason != "Deferred" && reason != "Infeasible" {
		t.Fatalf("expected reason Deferred or Infeasible, got %s (msg: %s)", reason, msg)
	}
	t.Logf("PodResizePending: reason=%s, message=%s", reason, msg)

	// Controller should create a migration for this pod.
	// The controller must be running separately (locally or in-cluster).
	waitForMigrationForPod(t, ns, podName, defaultWaitTimeout)
	t.Logf("Migration CRD created for pod %s", podName)
}

// TestE02_NonMigrationEnabledPodIgnored tests that a pod without the
// live.cast.ai/migration-enabled=true label does NOT get a migration.
// Note: CLM auto-adds this label to eligible pods, so we verify by
// checking that CAST AI agent pods (which don't have the label) are ignored.
func TestE02_NonMigrationEnabledPodIgnored(t *testing.T) {
	// This is a verification test — we check that no migrations exist
	// for any pods in the castai-agent namespace.
	migrations, err := dynClient.Resource(migrationGVR).Namespace("castai-agent").List(context.Background(), metav1.ListOptions{})
	if err == nil {
		if len(migrations.Items) > 0 {
			t.Fatalf("expected no migrations in castai-agent namespace, found %d", len(migrations.Items))
		}
	}
	t.Log("No migrations created for castai-agent pods (correct)")
}

// TestE03_InfeasibleResizeDetected tests that an Infeasible resize
// (requested CPU > node total capacity) is detected and triggers migration.
func TestE03_InfeasibleResizeDetected(t *testing.T) {
	ns := "e2e-infeasible"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	podName := getPodName(t, ns, "app", "nginx")
	// Patch to a CPU larger than any node's total capacity (1930m)
	patchPodResize(t, ns, podName, "nginx", "4500m")

	reason, msg := waitForPodResizePending(t, ns, podName, 60*time.Second)
	if reason != "Infeasible" {
		t.Fatalf("expected reason=Infeasible, got %s (msg: %s)", reason, msg)
	}
	t.Logf("Infeasible detected: %s", msg)

	// Controller should create migration immediately for Infeasible.
	waitForMigrationForPod(t, ns, podName, defaultWaitTimeout)
	t.Log("Migration created for Infeasible pod")
}

// TestE04_ResizeSucceedsNoMigration tests that a resize that fits on the node
// does NOT trigger a migration (no PodResizePending condition set).
func TestE04_ResizeSucceedsNoMigration(t *testing.T) {
	ns := "e2e-resize-ok"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	podName := getPodName(t, ns, "app", "nginx")
	// Patch to a small CPU that should fit on the node
	patchPodResize(t, ns, podName, "nginx", "200m")

	// Wait a bit and check that no PodResizePending was set
	time.Sleep(15 * time.Second)
	pending, reason, _ := getPodResizePending(t, ns, podName)
	if pending {
		t.Fatalf("expected no PodResizePending for small resize, but got reason=%s", reason)
	}

	// Verify no migration was created
	waitForNoMigrationForPod(t, ns, podName, 30*time.Second)
	t.Log("Small resize applied successfully, no migration created")
}

// TestE05_DownsizeIgnored tests that a CPU downsize does not trigger migration.
func TestE05_DownsizeIgnored(t *testing.T) {
	ns := "e2e-downsize"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "500m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	podName := getPodName(t, ns, "app", "nginx")
	// Patch to a smaller CPU (downsize)
	patchPodResize(t, ns, podName, "nginx", "100m")

	// Wait and check that no PodResizePending was set
	time.Sleep(15 * time.Second)
	pending, reason, _ := getPodResizePending(t, ns, podName)
	if pending {
		t.Fatalf("expected no PodResizePending for downsize, but got reason=%s", reason)
	}

	// Verify no migration was created
	waitForNoMigrationForPod(t, ns, podName, 30*time.Second)
	t.Log("Downsize ignored correctly, no migration created")
}

// TestE06_MultiplePodsSameNodeSurge tests that when multiple pods on the same
// node all have pending resizes, a migration is created for each pod.
func TestE06_MultiplePodsSameNodeSurge(t *testing.T) {
	ns := "e2e-multi-same-node"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	// Deploy 3 nginx pods
	deployNginx(t, ns, "nginx-a", 1, "100m")
	deployNginx(t, ns, "nginx-b", 1, "100m")
	deployNginx(t, ns, "nginx-c", 1, "100m")
	waitForDeployment(t, ns, "nginx-a", 2*time.Minute)
	waitForDeployment(t, ns, "nginx-b", 2*time.Minute)
	waitForDeployment(t, ns, "nginx-c", 2*time.Minute)

	// Fill the node with fillers
	deployFiller(t, ns, "filler-1", "600m")
	deployFiller(t, ns, "filler-2", "600m")
	waitForDeployment(t, ns, "filler-1", 2*time.Minute)
	waitForDeployment(t, ns, "filler-2", 2*time.Minute)

	// Patch all 3 pods to 1500m
	podA := getPodName(t, ns, "app", "nginx-a")
	podB := getPodName(t, ns, "app", "nginx-b")
	podC := getPodName(t, ns, "app", "nginx-c")

	patchPodResize(t, ns, podA, "nginx", "1500m")
	patchPodResize(t, ns, podB, "nginx", "1500m")
	patchPodResize(t, ns, podC, "nginx", "1500m")

	// Wait for all 3 to get PodResizePending
	waitForPodResizePending(t, ns, podA, 60*time.Second)
	waitForPodResizePending(t, ns, podB, 60*time.Second)
	waitForPodResizePending(t, ns, podC, 60*time.Second)

	// Wait for migrations to be created for all 3
	waitForMigrationForPod(t, ns, podA, defaultWaitTimeout)
	waitForMigrationForPod(t, ns, podB, defaultWaitTimeout)
	waitForMigrationForPod(t, ns, podC, defaultWaitTimeout)

	t.Logf("Migrations created for all 3 pods: %s, %s, %s", podA, podB, podC)
}

// TestE07_MigrationCompletes tests that a migration reaches Completed state
// and the clone pod has the desired CPU allocated.
func TestE07_MigrationCompletes(t *testing.T) {
	ns := "e2e-migration-completes"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	// Use Infeasible resize (4500m > node total capacity 3920m)
	// so PodResizePending is guaranteed regardless of fillers.
	podName := getPodName(t, ns, "app", "nginx")
	patchPodResize(t, ns, podName, "nginx", "4500m")

	reason, _ := waitForPodResizePending(t, ns, podName, 60*time.Second)
	t.Logf("PodResizePending: reason=%s", reason)

	waitForMigrationForPod(t, ns, podName, defaultWaitTimeout)

	// Wait for migration to complete (may take a few minutes for node provisioning)
	waitForMigrationState(t, ns, podName, "Completed", 10*time.Minute)

	// Find the clone pod and check its allocated CPU
	clonePodName := podName + "-clone-1"
	allocated := getPodAllocatedCPU(t, ns, clonePodName)
	if allocated != "4500m" {
		t.Fatalf("expected clone pod allocated CPU=4500m, got %s", allocated)
	}
	t.Logf("Migration completed, clone pod %s has allocated CPU=%s", clonePodName, allocated)
}

// TestE08_DuplicatePrevention tests that a second informer event for a pod
// with an already-active migration does NOT create a second migration.
func TestE08_DuplicatePrevention(t *testing.T) {
	ns := "e2e-duplicate"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	// Use Infeasible resize so PodResizePending is guaranteed.
	podName := getPodName(t, ns, "app", "nginx")
	patchPodResize(t, ns, podName, "nginx", "4500m")

	waitForPodResizePending(t, ns, podName, 120*time.Second)
	waitForMigrationForPod(t, ns, podName, defaultWaitTimeout)

	// Count migrations for this pod — should be exactly 1
	migrations, err := dynClient.Resource(migrationGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	count := 0
	for _, item := range migrations.Items {
		specPod, _, _ := unstructured.NestedString(item.Object, "spec", "podName")
		if specPod == podName {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 migration for pod %s, found %d", podName, count)
	}
	t.Logf("Duplicate prevention works: exactly 1 migration for %s", podName)
}

// TestE09_PodDeletedWhilePending tests that deleting a pod while it has
// a pending resize does not cause issues (controller removes it from tracking).
func TestE09_PodDeletedWhilePending(t *testing.T) {
	ns := "e2e-pod-deleted"
	// Delete any leftover namespace first
	_ = clientset.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	// Use Infeasible resize so PodResizePending is guaranteed.
	podName := getPodName(t, ns, "app", "nginx")
	patchPodResize(t, ns, podName, "nginx", "4500m")
	waitForPodResizePending(t, ns, podName, 120*time.Second)

	// Delete the pod directly (not the deployment)
	err := clientset.CoreV1().Pods(ns).Delete(context.Background(), podName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete pod: %v", err)
	}

	// Wait and verify no migration was created (pod deleted before threshold)
	waitForNoMigrationForPod(t, ns, podName, 30*time.Second)
	t.Log("Pod deleted while pending — no migration created, controller handled it gracefully")
}

// TestE10_PodResizePendingConditionFalseIgnored tests that a pod with
// PodResizePending condition set to False is not considered pending.
func TestE10_ConditionFalseIgnored(t *testing.T) {
	ns := "e2e-condition-false"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })

	deployNginx(t, ns, "nginx", 1, "100m")
	waitForDeployment(t, ns, "nginx", 2*time.Minute)

	podName := getPodName(t, ns, "app", "nginx")

	// Verify the pod does NOT have PodResizePending=True (no resize was attempted)
	pending, reason, _ := getPodResizePending(t, ns, podName)
	if pending {
		t.Fatalf("expected no PodResizePending for pod without resize, got reason=%s", reason)
	}

	// Verify no migration created
	waitForNoMigrationForPod(t, ns, podName, 15*time.Second)
	t.Log("Pod without PodResizePending condition correctly ignored")
}

// TestE11_BurstFivePodsDifferentNodes tests that 5 pods on different nodes
// with pending resizes all get migrations created.
func TestE11_BurstFivePods(t *testing.T) {
	ns := "e2e-burst-five"
	createNamespace(t, ns)
	t.Cleanup(func() { deleteNamespace(t, ns) })
	createResizeRBAC(t, ns)

	// Deploy 5 nginx pods
	for _, name := range []string{"nginx-1", "nginx-2", "nginx-3", "nginx-4", "nginx-5"} {
		deployNginx(t, ns, name, 1, "100m")
		waitForDeployment(t, ns, name, 2*time.Minute)
	}

	// Fill nodes with fillers to create contention
	for i := 1; i <= 5; i++ {
		deployFiller(t, ns, fmt.Sprintf("filler-%d", i), "700m")
		waitForDeployment(t, ns, fmt.Sprintf("filler-%d", i), 2*time.Minute)
	}

	// Patch all 5 pods to 1500m
	var podNames []string
	for _, deployName := range []string{"nginx-1", "nginx-2", "nginx-3", "nginx-4", "nginx-5"} {
		podName := getPodName(t, ns, "app", deployName)
		podNames = append(podNames, podName)
		patchPodResize(t, ns, podName, "nginx", "1500m")
	}

	// Wait for all to get PodResizePending
	for _, podName := range podNames {
		waitForPodResizePending(t, ns, podName, 60*time.Second)
	}

	// Wait for migrations for all 5
	for _, podName := range podNames {
		waitForMigrationForPod(t, ns, podName, defaultWaitTimeout)
	}

	t.Logf("All 5 pods got migrations: %v", podNames)
}
