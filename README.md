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

## Configuration

| Env var | Default | Description |
|---|---|---|
| `DRY_RUN` | `true` | If `true`, logs migrations but does not create CRDs. |
| `PENDING_THRESHOLD` | `2m` | How long a resize must be pending before creating a migration. |
| `NODE_DELTA_THRESHOLD` | `0.15` | Minimum pending delta / node allocatable CPU ratio. |
| `MIGRATION_TIMEOUT` | `10m` | How long a migration is considered active. |
| `MIGRATION_RETRY_LIMIT` | `3` | Max retries per failed migration. |
| `MIGRATION_ALERT_THRESHOLD` | `3` | Migrations per workload per hour before alerting. |
| `CLM_NODE_TEMPLATE` | `clm-live-migration-template` | Name of the CLM-enabled node template (informational). |

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

## Prerequisites

The customer's CAST AI cluster must have a CLM-enabled node template configured with:

- `clmEnabled: true`
- Single architecture (`amd64`)
- `imageFamily: FAMILY_AL2023` in the linked node configuration
- `containerRuntime: containerd`
- Custom label `live.cast.ai/install=true`

Source pods must be scheduled on nodes created from that template.

## Project layout

```
castai-workload-resize-migrator/
├── cmd/castai-workload-resize-migrator/   # Main entry point
├── pkg/
│   ├── config/                            # Environment-based config
│   ├── detector/                          # Pending resize detection
│   └── migrator/                          # CAST AI Migration CRD creation
├── k8s/
│   └── deployment.yaml                    # In-cluster deployment manifest
└── Dockerfile
```
