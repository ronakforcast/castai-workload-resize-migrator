# castai-workload-resize-migrator

Detects when CAST AI Workload Autoscaler (WOO) cannot apply a CPU upsize recommendation to a pod because the node is full, and triggers CAST AI Container Live Migration (CLM) by creating a `Migration` CRD. CAST AI CLM then provisions a suitable node and live-migrates the pod so the upsize can be applied.

## Why

WOO can recommend larger CPU requests for Coder pods, but if the node is full, Kubernetes defers the in-place resize (`desired > allocated`). The pod stays stuck at the old allocation. CAST AI Container Live Migration can move the pod to a node with enough room, allowing WOO to apply the upsize.

This controller focuses only on **detecting the stuck resize** and **triggering the migration**. Node provisioning, capacity pod creation, checkpoint/restore, and live migration are handled by CAST AI CLM.

## How it works

1. Watches all pods via Kubernetes informers.
2. Detects pods where `spec.containers[].resources.requests.cpu` (desired) > `status.containerStatuses[].allocatedResources.cpu` (allocated).
3. Waits for a configurable pending threshold (default 2 minutes).
4. Aggregates pending CPU delta per node and flags pods on nodes that cannot fit the delta.
5. Creates a `live.cast.ai/v1 Migration` CRD for each suspect pod.
6. Skips pods that already have an active migration.

## Prerequisites

### 1. Cluster requirements

- CAST AI-managed AWS EKS cluster running Kubernetes 1.30+
- In-place pod vertical scaling enabled (Kubernetes 1.33+ or feature gate `InPlacePodVerticalScaling`)
- CAST AI Container Live Migration (CLM) installed and enabled
- CAST AI autoscaler enabled with **Unschedulable Pods** policy turned on

### 2. CLM node template

The customer must create a CAST AI node template with Container Live Migration enabled:

- **Container Live Migration**: enabled
- **Processor architecture**: single (`amd64` or `arm64`, not "Any")
- **Compatible instance families**: from the same generation (e.g., c5, m5, r5)
- **Single subnet / single AZ**: CLM requires source and destination nodes in the same Availability Zone

### 3. Node configuration

The linked node configuration must use:

- **Image family**: `Amazon Linux 2023` (`FAMILY_AL2023`)
- **Container runtime**: `containerd`

### 4. Workload requirements

Source pods must:

- Be scheduled on nodes created from the CLM-enabled node template
- Have `resizePolicy` with `restartPolicy: NotRequired` for CPU
- Not use hard `kubernetes.io/hostname` node selectors (use `scheduling.cast.ai/node-template` instead)

## Installation

### Using Helm

```bash
helm install castai-workload-resize-migrator ./helm \
  --namespace castai-workload-resize-migrator \
  --create-namespace \
  --set config.dryRun=false
```

### Using kubectl (without Helm)

```bash
kubectl apply -f k8s/deployment.yaml
```

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/ronakforcast/castai-workload-resize-migrator` | Container image repository |
| `image.tag` | `0.1.0` | Container image tag |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `replicaCount` | `1` | Number of replicas (leader election ensures single active) |
| `namespace` | `castai-workload-resize-migrator` | Namespace to deploy into |
| `config.dryRun` | `true` | If true, logs migrations but does not create CRDs |
| `config.pendingThreshold` | `2m` | How long a resize must be pending before triggering migration |
| `config.nodeDeltaThreshold` | `0.15` | Minimum pending delta / node allocatable CPU ratio |
| `config.migrationTimeout` | `10m` | How long a migration is considered active |
| `config.migrationRetryLimit` | `3` | Max retries per failed migration |
| `config.migrationAlertThreshold` | `3` | Migrations per workload per hour before alerting |
| `config.clmNodeTemplate` | `clm-live-migration-template` | Name of the CLM-enabled node template |
| `config.leaderElection` | `true` | Enable leader election for HA |
| `resources.requests.cpu` | `50m` | CPU request |
| `resources.requests.memory` | `64Mi` | Memory request |
| `resources.limits.memory` | `256Mi` | Memory limit |

### Example: custom values

```yaml
# values.yaml
config:
  dryRun: false
  pendingThreshold: "1m"
  nodeDeltaThreshold: "0.10"
  migrationTimeout: "5m"
  clmNodeTemplate: "my-clm-template"

resources:
  requests:
    cpu: "100m"
    memory: "128Mi"
  limits:
    memory: "512Mi"
```

```bash
helm install castai-workload-resize-migrator ./helm \
  --namespace castai-workload-resize-migrator \
  --create-namespace \
  -f values.yaml
```

## Running locally

```bash
env DRY_RUN=true \
  PENDING_THRESHOLD=5s \
  NODE_DELTA_THRESHOLD=0.10 \
  go run ./cmd/castai-workload-resize-migrator -kubeconfig "$HOME/.kube/config"
```

## Testing

```bash
go test ./...
```

## How to verify it's working

1. Check controller logs:
   ```bash
   kubectl logs -n castai-workload-resize-migrator deployment/castai-workload-resize-migrator
   ```

2. Watch for created migrations:
   ```bash
   kubectl get migrations -A -w
   ```

3. Check migration status:
   ```bash
   kubectl describe migration <name> -n <namespace>
   ```

## Troubleshooting

| Issue | Fix |
|---|---|
| Migration fails with `PodSchedulingFailed` | Ensure source and destination nodes are in the **same AZ** (single subnet in node config) |
| Migration fails with `SubnetMismatch` | Add `topology.cast.ai/subnet-id` label to EKS-managed nodes, or use only CLM-provisioned nodes |
| Capacity pod stuck Pending | Ensure the CLM node template allows large enough instances for the desired CPU |
| Controller detects no suspect pods | Ensure pods have `resizePolicy` and `allocatedResources` in their status (requires in-place resize support) |
| Autoscaler doesn't provision nodes | Ensure **Unschedulable Pods** policy is enabled in CAST AI autoscaler settings |

## Project layout

```
castai-workload-resize-migrator/
├── cmd/castai-workload-resize-migrator/   # Main entry point
├── pkg/
│   ├── config/                            # Environment-based config
│   ├── detector/                          # Pending resize detection
│   └── migrator/                          # CAST AI Migration CRD creation
├── helm/                                  # Helm chart
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
│       ├── _helpers.tpl
│       ├── namespace.yaml
│       └── deployment.yaml
├── k8s/
│   └── deployment.yaml                    # Plain k8s manifest (without Helm)
├── Dockerfile
└── README.md
```
