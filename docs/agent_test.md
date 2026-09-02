# Agent Testing Guide

This document captures every test we ran, why we ran it, the exact commands, and the expected results. The goal is that any agent (or human) can reproduce the entire validation from scratch.

> **Environment used in this guide**
> - Local cluster: `k3d-local`
> - EKS test cluster: `eks-250802-rp` (EKS 1.35.6, `eu-central-1`)
> - CAST AI cluster ID: `b26886da-4798-47a7-b60f-f8025b901b09`
> - CAST AI node config: `clm-live-migration-config` (`f2934351-0901-4d0f-b081-64089eb9851a`)
> - CAST AI node template: `clm-live-migration-template`
> - Repo path: `/Users/dhavalthakkar/harman-clm-solution/woop-rebalance-controller`

Replace region, cluster names, and IDs with your own where necessary.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Quick Start: Local k3d Cluster](#2-quick-start-local-k3d-cluster)
3. [EKS Cluster Setup & Onboarding](#3-eks-cluster-setup--onboarding)
4. [Unit Tests](#4-unit-tests)
5. [User Stories](#5-user-stories)
6. [Component Tests](#6-component-tests)
7. [Migration Experiments](#7-migration-experiments)
8. [Troubleshooting](#8-troubleshooting)
9. [Test Results Summary](#9-test-results-summary)

---

## 1. Prerequisites

### 1.1 Tools

```bash
# Verify installed
k3d version
kubectl version --client
eksctl version
castctl version
go version
docker --version
```

### 1.2 CAST AI API key

Export before any `castctl` command:

```bash
export CASTAI_API_KEY="<your-key>"
```

### 1.3 AWS credentials

Ensure `aws configure` is set or `AWS_PROFILE` is exported for `eksctl`.

---

## 2. Quick Start: Local k3d Cluster

### 2.1 Check if cluster exists

```bash
k3d cluster list
```

If `k3d-local` is missing:

```bash
k3d cluster create k3d-local \
  --servers 1 \
  --agents 2 \
  --no-lb \
  --k3s-arg "--kubelet-arg=feature-gates=InPlacePodVerticalScaling=true@all:*"
```

> **Note:** In-place resize requires Kubernetes 1.33+ or the `InPlacePodVerticalScaling` feature gate. k3d may not support it on older k3s versions.

### 2.2 Verify cluster

```bash
kubectl config current-context
kubectl get nodes
```

### 2.3 Onboard to CAST AI

```bash
castctl cluster connect --name k3d-local --provider k3s
```

Verify in CAST AI console or via:

```bash
castctl cluster list
```

### 2.4 Install CAST AI components

Follow CAST AI console instructions for the connected cluster. Typically:

```bash
# Workload Autoscaler and CLM are installed from the CAST AI console or Helm chart.
# Check the cluster page for the exact install command.
```

---

## 3. EKS Cluster Setup & Onboarding

### 3.1 Check if EKS cluster exists

```bash
eksctl get cluster --region eu-central-1
```

If `eks-250802-rp` is missing:

```bash
eksctl create cluster -f .kimchi/docs/eks-250802-rp-eksctl.yaml
```

### 3.2 Get kubeconfig

```bash
aws eks update-kubeconfig --name eks-250802-rp --region eu-central-1
kubectl config current-context
```

### 3.3 Onboard to CAST AI

```bash
castctl cluster connect --name eks-250802-rp --provider eks --region eu-central-1
```

Note the CAST AI cluster ID returned (ours was `b26886da-4798-47a7-b60f-f8025b901b09`).

### 3.4 Verify CAST AI agent is running

```bash
kubectl get pods -n castai-agent
kubectl logs -n castai-agent deployment/castai-agent --tail=50
```

### 3.5 Enable required CAST AI policies

In CAST AI console:
- **Unschedulable Pods** policy: ON
- **Workload Autoscaler**: ON
- **Container Live Migration**: installed and enabled

---

## 4. Unit Tests

### 4.1 Run all tests

```bash
cd /Users/dhavalthakkar/harman-clm-solution/woop-rebalance-controller
go test ./...
```

### 4.2 Run with coverage

```bash
go test ./... -cover
```

### 4.3 Run specific packages

```bash
go test ./pkg/config/... -v
go test ./pkg/detector/... -v
go test ./pkg/migrator/... -v
```

### 4.4 Expected result

```text
ok      castai-workload-resize-migrator/pkg/config        0.123s  coverage: 80.0%
ok      castai-workload-resize-migrator/pkg/detector      0.234s  coverage: 75.0%
ok      castai-workload-resize-migrator/pkg/migrator      0.345s  coverage: 60.0%
```

---

## 5. User Stories

### US-1: Pod resize gets stuck because the node is full

**Goal:** Verify the controller can detect a pod whose desired CPU is greater than allocated CPU.

**Steps:**

1. Deploy a test pod with CPU request 100m on a packed node:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: resize-test
  namespace: default
spec:
  nodeSelector:
    scheduling.cast.ai/node-template: clm-live-migration-template
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "sleep 3600"]
    resources:
      requests:
        cpu: "100m"
      resizePolicy:
      - resourceName: cpu
        restartPolicy: NotRequired
EOF
```

2. Verify pod is running:

```bash
kubectl get pod resize-test -o wide
```

3. Patch CPU via `/resize` subresource:

```bash
kubectl patch pod resize-test --subresource=resize --patch '
{
  "spec": {
    "containers": [{
      "name": "app",
      "resources": {"requests": {"cpu": "400m"}}
    }]
  }
}'
```

4. Verify `PodResizePending`:

```bash
kubectl get pod resize-test -o jsonpath='{.status.conditions[?(@.type=="PodResizePending")]}'
```

**Expected:** `status=True`, `reason=Deferred` or `reason=Infeasible` depending on node capacity.

5. Run controller in dry-run:

```bash
cd /Users/dhavalthakkar/harman-clm-solution/woop-rebalance-controller
DRY_RUN=true go run ./cmd/castai-workload-resize-migrator -kubeconfig "$HOME/.kube/config"
```

**Expected log:**

```text
suspect pod detected ... allocated=100m desired=400m
DRY-RUN: would create migration for pod default/resize-test
```

### US-2: Migration CRD triggers CLM node provisioning

**Goal:** Verify that creating a `Migration` CRD makes CLM provision a new node.

**Prerequisites:**
- CLM-enabled node template exists
- Custom label `live.cast.ai/install=true` on template
- Node config uses `FAMILY_AL2023` + `containerd`

**Steps:**

1. Ensure pod has label `live.cast.ai/migration-enabled=true` (CLM adds this automatically to eligible pods).

2. Create a Migration CRD:

```bash
kubectl apply -f - <<EOF
apiVersion: live.cast.ai/v1
kind: Migration
metadata:
  name: resize-test-migration
  namespace: default
spec:
  podName: resize-test
  podNamespace: default
  destination: <current-node-name>
EOF
```

3. Watch for capacity pod and node:

```bash
kubectl get pods -n default -w
kubectl get nodes -w
```

**Expected:** A capacity pod appears with desired CPU, then a new node is provisioned by CAST AI autoscaler.

4. Check migration status:

```bash
kubectl describe migration resize-test-migration -n default
```

### US-3: Migration fails safely when prerequisites are missing

**Goal:** Confirm the controller doesn't cause harm when CLM cannot satisfy the migration.

**Steps:**

1. Delete the CLM node template or use a pod with hard `kubernetes.io/hostname` selector.

2. Trigger resize and create Migration CRD.

3. Observe CAST AI autoscaler logs:

```bash
kubectl logs -n castai-agent deployment/castai-agent | grep -i "impossible\|failed\|error"
```

**Expected:** Migration stays in `WaitingForCapacity` or fails with clear reason. No infinite node creation.

---

## 6. Component Tests

### CT-1: Verify in-place resize is supported

```bash
kubectl api-resources | grep resize
kubectl get --raw /api/v1 | grep -i resize
```

**Expected:** `/resize` subresource is available on EKS 1.35+.

### CT-2: Verify `allocatedResources` field

```bash
kubectl get pod resize-test -o jsonpath='{.status.containerStatuses[0].allocatedResources}'
```

**Expected:** Shows current allocated CPU, different from `resources.requests` during pending resize.

### CT-3: Verify `PodResizePending` reason

```bash
kubectl get pod resize-test -o jsonpath='{.status.conditions[?(@.type=="PodResizePending")].reason}'
```

**Expected values:**
- `Deferred` — node temporarily full
- `Infeasible` — desired CPU > node capacity

### CT-4: Verify controller builds

```bash
cd /Users/dhavalthakkar/harman-clm-solution/woop-rebalance-controller
go build ./cmd/castai-workload-resize-migrator
```

**Expected:** Binary built successfully, no errors.

### CT-5: Verify controller safety scan

Run controller and leave it running. It should log every configured interval:

```text
running safety scan
safety scan found no suspect pods
```

When a suspect pod appears, it should log the plan within one interval.

---

## 7. Migration Experiments

### MIG-01: Hard hostname nodeSelector

**Setup:** Pod has `nodeSelector: {kubernetes.io/hostname: ip-10-0-11-102}`

**Expected result:** ❌ Capacity pod unschedulable. Hard hostname selector is copied to capacity pod and conflicts with anti-affinity to source node.

**Verification:**

```bash
kubectl get pods -n default
kubectl describe pod <capacity-pod-name>
# Events show: node affinity didn't match
```

### MIG-02: `live.cast.ai/install=true` without CLM template

**Setup:** Pod has `nodeSelector: {live.cast.ai/install: "true"}`, no CLM-enabled template.

**Expected result:** ❌ No node provisioned. Autoscaler logs: `Impossible to schedule pod, no instance type matches selectors`.

**Verification:**

```bash
kubectl logs -n castai-agent deployment/castai-agent | grep -i "impossible"
```

### MIG-03: No CLM template, desired 1500m

**Setup:** Same as MIG-02 with desired CPU 1500m.

**Expected result:** ❌ Capacity pod created with 1500m, but no node. Stuck `WaitingForCapacity`.

### MIG-04: With CLM template, busybox 2500m

**Setup:**
- Node template: `clm-live-migration-template` with `clmEnabled: true`
- Pod scheduled via `scheduling.cast.ai/node-template: clm-live-migration-template`
- Desired CPU: 2500m

**Expected result:** ⚠️ Autoscaler creates `c5a.xlarge` node. Migration restore step may fail with `PodSchedulingFailed`.

**Verification:**

```bash
kubectl get nodes -w
kubectl describe migration <name>
```

### MIG-05: With CLM template, nginx 4500m

**Setup:** Same as MIG-04, nginx pod, desired CPU 4500m.

**Expected result:** ⚠️ Autoscaler creates `c5a.2xlarge` node. Migration provisioning confirmed.

---

## 8. Troubleshooting

### Issue: Controller detects no suspect pods

**Check:**

```bash
kubectl get pod <pod> -o jsonpath='{.status.conditions[?(@.type=="PodResizePending")]}'
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].allocatedResources}'
kubectl get pod <pod> -o jsonpath='{.spec.containers[0].resources.requests}'
```

**Fix:** Ensure in-place resize is enabled and pod has `resizePolicy` with `restartPolicy: NotRequired` for CPU.

### Issue: Migration fails with `SubnetMismatch`

**Fix:** Ensure EKS-managed nodes have `topology.cast.ai/subnet-id` label, or use only CLM-provisioned nodes. Node config must use a single subnet.

### Issue: Capacity pod stuck Pending

**Check:**

```bash
kubectl describe pod <capacity-pod>
kubectl logs -n castai-agent deployment/castai-agent | grep -i "capacity\|impossible"
```

**Fix:** Ensure CLM node template allows large enough instance families and has `live.cast.ai/install=true` label.

### Issue: `go.mod` version invalid

**Fix:** Set to a real version, e.g.:

```bash
go mod edit -go=1.24
```

---

## 9. Test Results Summary

| Category | Test | Result |
|---|---|---|
| Unit tests | Config, detector, migrator | ✅ 29 passed |
| EKS 1.31 | In-place resize support | ❌ Not supported |
| EKS 1.35 | In-place resize support | ✅ Supported |
| Controller | Dry-run detection | ✅ Detected suspect pod |
| Migration | Hard hostname selector | ❌ Capacity pod unschedulable |
| Migration | No CLM template | ❌ No node provisioned |
| Migration | CLM template + busybox 2500m | ⚠️ Node created, restore failed |
| Migration | CLM template + nginx 4500m | ⚠️ Node created, in progress |

**Open validations:**
- Full in-cluster non-dry-run controller test.
- End-to-end: resize stuck → migration created → CLM live-migrates → resize applied.
- Migration retry and alert threshold behavior.
- Docker image build and push.
