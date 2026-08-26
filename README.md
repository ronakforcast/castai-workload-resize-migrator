# castai-workload-resize-migrator

Detects when CAST AI Workload Autoscaler (WOO) cannot apply a CPU upsize recommendation to a pod because the node is full, and triggers CAST AI Container Live Migration (CLM) by creating a `Migration` CRD. CAST AI CLM then provisions a suitable node and live-migrates the pod so the upsize can be applied.

---

## Table of Contents

- [Why](#why)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Project Layout](#project-layout)

---

## Why

WOO can recommend larger CPU requests for Coder pods, but if the node is full, Kubernetes defers the in-place resize (`desired > allocated`). The pod stays stuck at the old allocation. CAST AI Container Live Migration can move the pod to a node with enough room, allowing WOO to apply the upsize.

This controller focuses only on **detecting the stuck resize** and **triggering the migration**. Node provisioning, capacity pod creation, checkpoint/restore, and live migration are handled by CAST AI CLM.

---

## Architecture

### High-level flow

```
┌──────────┐    ┌──────────┐    ┌──────────────────┐    ┌──────────┐    ┌──────────┐
│   WOO    │───▶│   Pod    │───▶│  desired >       │───▶│  CLM     │───▶│  Pod on  │
│ recommends│   │ patched  │    │  allocated?       │    │ Migration│    │ new node │
│ CPU upsize│   │ via      │    │                    │    │ + node   │    │ resize   │
│           │   │ /resize  │    │  Controller       │    │ provision│    │ applied  │
└──────────┘    └──────────┘    └──────────────────┘    └──────────┘    └──────────┘
                                      │
                                      │ detects & triggers
                                      ▼
                            ┌──────────────────┐
                            │  Safety Scan     │
                            │  (every 1 min)   │
                            └──────────────────┘
```

### Component diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Controller Pod                           │
│                                                             │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────────┐  │
│  │  Informers   │──▶│   Detector   │──▶│    Migrator     │  │
│  │  (Pod/Node)  │   │              │   │                 │  │
│  │              │   │ • Filter on   │   │ • Create        │  │
│  │  Watch all   │   │   migration-  │   │   Migration CRD │  │
│  │  pods/nodes  │   │   enabled    │   │ • Track status  │  │
│  │              │   │   label      │   │ • Retry on fail │  │
│  │              │   │ • Compare     │   │ • Cleanup       │  │
│  │              │   │   desired vs │   │   completed     │  │
│  │              │   │   allocated  │   │                 │  │
│  │              │   │ • Aggregate  │   │                 │  │
│  │              │   │   per node   │   │                 │  │
│  └─────────────┘   └──────────────┘   └────────┬────────┘  │
│          │                                     │           │
│          │                                     ▼           │
│          │                          ┌─────────────────┐    │
│          │                          │  Dynamic Client  │    │
│          │                          │  (Migration CRD) │    │
│          │                          └─────────────────┘    │
│          ▼                                                 │
│  ┌─────────────┐                                           │
│  │ Safety Scan  │                                           │
│  │ (fallback)   │                                           │
│  │ every 1 min  │                                           │
│  └─────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes API                           │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────┐  │
│  │   Pods    │  │  Nodes   │  │  Migration CRD           │  │
│  │           │  │          │  │  (live.cast.ai/v1)       │  │
│  └──────────┘  └──────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Detection and migration flow

```
Pod Update (via informer)
    │
    ▼
┌─────────────────────────────┐
│ Has live.cast.ai/           │─── No ──▶ Skip (ignore pod)
│ migration-enabled=true?     │
└─────────────┬───────────────┘
              │ Yes
              ▼
┌─────────────────────────────┐
│ spec.requests.cpu >         │─── No ──▶ Remove from pending
│ allocatedResources.cpu?     │
└─────────────┬───────────────┘
              │ Yes (pending resize)
              ▼
┌─────────────────────────────┐
│ Pending for >               │─── No ──▶ Wait (not yet)
│ PENDING_THRESHOLD?          │
└─────────────┬───────────────┘
              │ Yes
              ▼
┌─────────────────────────────┐
│ Safety Scan (every 1 min):  │
│                             │
│ 1. Refresh node CPU sums    │
│ 2. Aggregate pending pods   │
│    per node                 │
│ 3. Check: node available    │
│    CPU < pending delta?     │
└─────────────┬───────────────┘
              │ Yes (node is full)
              ▼
┌─────────────────────────────┐
│ Migration already active    │─── Yes ──▶ Check if failed
│ for this pod?               │           & retryable
└─────────────┬───────────────┘
              │ No
              ▼
┌─────────────────────────────┐
│ Create Migration CRD        │
│ Track as active              │
└─────────────────────────────┘
```

### Migration lifecycle

```
                  ┌──────────────┐
                  │ Migration    │
                  │ Created      │
                  └──────┬───────┘
                         │
                         ▼
              ┌──────────────────┐
              │ WaitingForCapacity│
              │ (CLM provisions   │
              │  node + capacity  │
              │  pod)             │
              └────────┬─────────┘
                       │
           ┌───────────┼───────────┐
           ▼           ▼           ▼
     ┌──────────┐ ┌──────────┐ ┌──────────┐
     │Completed │ │ Running  │ │ Timeout  │
     │(success) │ │(migrating)│ │(10 min)  │
     └────┬─────┘ └────┬─────┘ └────┬─────┘
          │            │            │
          ▼            ▼            ▼
     ┌──────────────────────────────────┐
     │ Controller checks status every   │
     │ 30 seconds via cleanup loop     │
     └──────────────────────────────────┘
          │
    ┌─────┼──────┐
    ▼     ▼      ▼
 Remove Retry   Remove
 from    (if    from
 track   under   track
         limit)
```

---

## How It Works

1. **Watches all pods** via Kubernetes informers.
2. **Filters** on `live.cast.ai/migration-enabled=true` label (auto-added by CLM to eligible pods).
3. **Detects** pods where `spec.containers[].resources.requests.cpu` (desired) > `status.containerStatuses[].allocatedResources.cpu` (allocated).
4. **Waits** for a configurable pending threshold (default 2 minutes).
5. **Safety scan** runs every 1 minute (configurable) as a fallback:
   - Aggregates pending CPU delta per node.
   - Flags pods on nodes that cannot fit the delta.
6. **Creates** a `live.cast.ai/v1 Migration` CRD for each suspect pod.
7. **Tracks** migration status and retries on failure (up to `MIGRATION_RETRY_LIMIT`).
8. **Cleans up** completed or permanently failed migrations from tracking.

---

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
- **Custom label**: `live.cast.ai/install=true`

### 3. Node configuration

The linked node configuration must use:

- **Image family**: `Amazon Linux 2023` (`FAMILY_AL2023`)
- **Container runtime**: `containerd`
- **Single subnet** (same AZ as source nodes)

### 4. Workload requirements

Source pods must:

- Be scheduled on nodes created from the CLM-enabled node template
- Have `resizePolicy` with `restartPolicy: NotRequired` for CPU
- Not use hard `kubernetes.io/hostname` node selectors (use `scheduling.cast.ai/node-template` instead)

---

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

---

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
| `config.nodeDeltaThreshold` | `0.15` | Minimum pending delta / node allocatable CPU ratio (15%) |
| `config.safetyScanInterval` | `1m` | How often the safety scan runs as a fallback |
| `config.migrationTimeout` | `10m` | How long a migration is considered active before expiring |
| `config.migrationRetryLimit` | `3` | Max retries per failed migration |
| `config.migrationRetryDelay` | `30s` | Minimum delay before retrying a failed migration |
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
  safetyScanInterval: "30s"
  migrationTimeout: "5m"
  migrationRetryLimit: 5
  migrationRetryDelay: "1m"
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

---

## Verification

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

---

## Troubleshooting

| Issue | Fix |
|---|---|
| Migration fails with `PodSchedulingFailed` | Ensure source and destination nodes are in the **same AZ** (single subnet in node config) |
| Migration fails with `SubnetMismatch` | Add `topology.cast.ai/subnet-id` label to EKS-managed nodes, or use only CLM-provisioned nodes |
| Capacity pod stuck Pending | Ensure the CLM node template allows large enough instances for the desired CPU |
| Controller detects no suspect pods | Ensure pods have `resizePolicy` and `allocatedResources` in their status (requires in-place resize support) |
| Controller detects no suspect pods | Ensure pods have `live.cast.ai/migration-enabled=true` label (CLM adds this automatically to eligible pods) |
| Autoscaler doesn't provision nodes | Ensure **Unschedulable Pods** policy is enabled in CAST AI autoscaler settings |
| Migration keeps failing and retrying | Check if desired CPU fits on the destination node after ~500m system overhead |
| Controller logs noise from capacity pods | Capacity pods inherit the `migration-enabled` label but are on nodes with room, so no migrations are created. Log noise is expected. |

---

## Project Layout

```
castai-workload-resize-migrator/
├── cmd/castai-workload-resize-migrator/   # Main entry point
├── pkg/
│   ├── config/                            # Environment-based config
│   ├── detector/                          # Pending resize detection
│   └── migrator/                          # CAST AI Migration CRD creation + status tracking
├── helm/                                  # Helm chart
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
│       ├── _helpers.tpl
│       ├── namespace.yaml
│       └── deployment.yaml
├── k8s/
│   └── deployment.yaml                    # Plain k8s manifest (without Helm)
├── docs/
│   └── test-cases.md                      # Test cases and results
├── Dockerfile
└── README.md
```
