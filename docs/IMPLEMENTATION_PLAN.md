# Kubernetes API Shim for Ephemeral Container Workloads

## Context

**Problem:** Run expensive, short-lived containerized workloads (osmdiffs: 6 CPU, 8GB RAM, 250GB ephemeral disk, ~6hr runtime weekly) on a budget VPS without paying for always-on infrastructure or managed Kubernetes node pools.

**Solution:** Build a lightweight Kubernetes API server that runs on a cheap VPS ($10-20/month) and:
- Accepts Kubernetes Job/CronJob manifests via Terraform
- Creates ephemeral Hetzner Cloud VMs per job (6 CPU, 8GB RAM, cx51 ~€4/job)
- Manages 250GB ephemeral block volumes (provisioned on-demand, deleted after)
- Runs containers via cloud-init + Docker inside VMs
- Exposes Kubernetes-compatible APIs so Terraform `kubernetes_provider` works natively
- Streams logs, metrics, and cost tracking via custom Kubernetes APIs

**Key constraints:**
- Absolute requirement: 250GB ephemeral scratch storage per job
- Cheap indie developer infrastructure (total ~€50-60/month for weekly jobs)
- Must survive control plane restarts (crash recovery via reconciliation loop)
- Must prevent resource leaks (volumes, VMs) if any component fails

---

## Architecture Overview

### Components

**1. Control Plane (Shim):** Rust binary running on a cheap VPS
   - Axum HTTP server (port 6443, TLS)
   - SQLite for persistent state (jobs, volumes, VMs, events, pricing)
   - Reconciliation loop (10-second ticks) for state management
   - Hetzner Cloud API client + pricing sync

**2. Worker VMs:** Short-lived Hetzner Cloud VMs (created per job)
   - Receive volume attachment + container spec via cloud-init
   - Run Docker container with mounted volume
   - Communicate status/logs back to shim via SSH + status files

**3. Kubernetes API Surface:** Minimal implementation
   - Resources: `Secret` (v1), `CronJob` (batch/v1), Pod status
   - Operations: CRUD on resources, watch/streaming, list
   - Extensions: Events, Metrics, Cost tracking (custom APIs)

### Data Flow

```
Terraform (terraform apply)
  → HTTPS POST to Shim:6443 (kubernetes_provider)
    → Shim stores Secret/CronJob in SQLite
    → Reconciliation loop detects new jobs
      → Creates 250GB Hetzner volume
      → Launches Hetzner VM with cloud-init
      → Cloud-init mounts volume, pulls image, docker run
      → Container writes to /scratch, streams logs
      → On exit: Shim detects completion, fetches logs, deletes VM+volume
    → Shim updates job status in SQLite
    → Terraform reads back status via GET requests
  → kubectl logs -f osmdiffs-weekly-...  (streams from VM via SSH)
  → kubectl get pod, kubectl describe, etc. (reads from Shim database)
```

---

## Implementation Phases

### Phase 1: Scaffold + HTTP API (Days 1-2)
**Goal:** Prove Terraform can CRUD resources via the shim.

**Deliverables:**
- New Rust project with `Cargo.toml` (axum, tokio, sqlx, serde, tracing)
- HTTP server: TLS, self-signed certs (via provision script)
- Kubernetes API types (Secret, CronJob, Pod, ObjectMeta, etc.)
- SQLite schema (jobs, secrets, events tables)
- Discovery endpoints: `GET /api/v1`, `GET /apis/batch/v1`
- CRUD handlers: create/read/patch/delete for Secret and CronJob (no logic, just storage)

**Files to create:**
- `Cargo.toml` - dependencies
- `src/main.rs` - server setup, routes
- `src/api/mod.rs` - API handlers
- `src/api/secret.rs` - Secret CRUD
- `src/api/cronjob.rs` - CronJob CRUD
- `src/db/mod.rs` - SQLite setup, migrations
- `src/db/schema.sql` - database schema
- `src/config.rs` - config parsing (toml)
- `config-dev.toml` - development config template

**Testing:**
```bash
cargo build
cargo run -- -c config-dev.toml
# From another terminal:
terraform apply (against localhost:6443)
kubectl get secret osmdiffs-s3-credentials
sqlite3 db.sqlite "SELECT * FROM jobs"
```

---

### Phase 2: Reconciliation Loop Skeleton (Days 2-3)
**Goal:** Build the state machine that will drive all orchestration.

**Deliverables:**
- Reconciliation loop: tokio::time::interval every 10 seconds
- Job state machine: defined states + transition logic
- Database updates for job status, volume_id, worker_vm_id, etc.
- Error handling + retry tracking per job
- Startup reconciliation (detect orphaned jobs from crashed shim)

**Files to create/modify:**
- `src/reconcile/mod.rs` - reconciliation loop entry point
- `src/reconcile/job.rs` - job-specific reconciliation steps
- `src/reconcile/startup.rs` - startup recovery logic
- `src/db/schema.sql` - add retry_count, last_error, volume_id, worker_vm_id columns

**No external API calls yet.** Just:
- Poll SQLite for jobs not in terminal state
- Mock state transitions (e.g., `Pending` → `VolumeCreating` → `VolumeCreated` etc.)
- Log state changes

**Testing:**
```bash
# Create a job via Terraform
terraform apply
# Watch logs to see reconciliation loop detect it and advance state
journalctl -u kube-shim -f
# Verify state progression in SQLite
sqlite3 db.sqlite "SELECT name, status, retry_count FROM jobs"
```

---

### Phase 3: Hetzner Integration (Dry-Run Mode) (Days 3-4)
**Goal:** Call real Hetzner API but don't actually create resources yet.

**Deliverables:**
- Hetzner client setup (hcloud crate)
- `DRY_RUN=true` config flag
- Reconciliation step: `VolumePending` → attempt volume creation (logged, not executed)
- Reconciliation step: `VMPending` → attempt VM creation (logged, not executed)
- Error handling for Hetzner API authentication failures

**Files to modify:**
- `src/hetzner/mod.rs` (new) - Hetzner client wrapper
- `src/hetzner/volumes.rs` (new) - volume operations
- `src/hetzner/servers.rs` (new) - server operations
- `src/reconcile/job.rs` - add DRY_RUN checks before API calls
- `src/config.rs` - add dry_run boolean
- `config-dev.toml` - set dry_run = true

**Testing:**
```bash
# Apply osmdiffs CronJob (small test version, 1GB volumes)
terraform apply
# Watch logs:
# "DRY-RUN: Would create volume kube-shim-vol-osmdiffs-weekly-20260915-abc size=1GB"
# "DRY-RUN: Would launch VM kube-shim-worker-osmdiffs-weekly-20260915-abc"
# Verify NO resources created in Hetzner console
```

---

### Phase 4: Real Hetzner Operations (Small Volumes) (Days 4-5)
**Goal:** Actually provision volumes, but small (1-10GB) for safety.

**Deliverables:**
- Set `dry_run = false` in config
- Real volume creation in reconciliation loop
- Volume attachment to VPS
- Error handling + retry logic with timeouts (5 min for volume creation)
- Orphan detection: scan Hetzner for volumes with our prefix, delete if not tracked in DB
- Database: track volume_id, volume_device, mount_point

**Files to modify:**
- `src/hetzner/volumes.rs` - implement real create/attach/detach/delete
- `src/reconcile/job.rs` - implement volume state steps (VolumeCreating, VolumeAttaching, VolumeAttached)
- `src/reconcile/orphan_scan.rs` (new) - periodic orphan detection + cleanup
- `src/db/schema.sql` - add volume_device, mount_point columns

**Testing:**
```bash
# Watch Hetzner console live
# terraform apply (with test job, 1GB volume, short timeout)
# Verify volume appears: kube-shim-vol-test-...
# Verify volume mounted on VPS: ssh root@vps ls -la /mnt/scratch-*
# terraform destroy
# Verify volume deleted (takes ~2 min)
# Repeat 5 times without orphans
```

**Exit criteria:**
- 10 cycles of create→delete volume with zero orphans
- Hetzner bill still near zero

---

### Phase 5: VM Provisioning + Cloud-Init (Days 5-6)
**Goal:** Launch actual Hetzner Cloud VMs with containers inside.

**Deliverables:**
- Cloud-init script generation (bash, templated with env vars)
- Real VM creation (cx51 or size based on job resources)
- VM waits for volume attachment, mounts it
- VM pulls container image, runs docker
- Container write to /scratch
- Status file: `/tmp/job-status.txt` (written by cloud-init on completion)
- Container ID saved: `/tmp/container-id.txt`
- Database: track worker_vm_id, worker_vm_name, worker_ssh_ip

**Files to create/modify:**
- `src/hetzner/servers.rs` - implement real server create/delete
- `src/cloud_init.rs` (new) - generate cloud-init script with proper escaping
- `src/reconcile/job.rs` - add VM state steps (VMCreating, VMRunning, etc.)
- `bootstrap/cloud-init-template.sh` (new) - bash template for VM startup
- `src/db/schema.sql` - add worker_vm_id, worker_ssh_ip, worker_vm_name, exit_code columns

**Test scenario:**
```bash
# Use busybox test image: writes 100MB to /scratch, exits 0
# terraform apply (test job with busybox)
# Watch:
#   1. Volume created + mounted
#   2. VM launched (watch Hetzner console)
#   3. VM IP becomes available (wait ~1 min for boot)
#   4. Container starts
#   5. Writes to /scratch
#   6. Container exits
# Verify logs fetchable: ssh root@{ip} docker logs {container-id}
# terraform destroy
# Verify VM + volume deleted
```

---

### Phase 6: Log Streaming (Days 6-7)
**Goal:** Support `kubectl logs -f` for live monitoring.

**Deliverables:**
- HTTP endpoint: `GET /api/v1/namespaces/default/pods/{name}/log?follow=true`
- SSH to worker VM, run `docker logs -f {container-id}`, stream back to client
- Fallback: return cached logs if job is not running
- Handle SSH disconnects gracefully

**Files to create/modify:**
- `src/api/logs.rs` (new) - log streaming handler
- `src/api/mod.rs` - register logs endpoint
- `src/ssh.rs` (new) - SSH helper functions

**Testing:**
```bash
# Start a job
terraform apply
# In another terminal
kubectl logs -f osmdiffs-weekly-test
# Should see:
# "Waiting for volume..."
# "Found device: /dev/sdc"
# "Mounting volume..."
# "Starting container..."
# ... container output ...
# "Exit code: 0"
```

---

### Phase 7: Reconciliation Hardening (Days 7-8)
**Goal:** Make the reconciliation loop bulletproof for production.

**Deliverables:**
- Comprehensive error handling for all Hetzner API calls
- Idempotent state transitions (can retry without side effects)
- Timeouts: volume creation (5 min), VM boot (5 min), container runtime (6 hours + job timeout)
- Cleanup on failure: unmount, detach, delete (even if one step fails)
- Startup recovery: detect crashed containers, orphaned volumes, incomplete jobs
- Chaos testing scenario: kill shim mid-job, restart, verify cleanup proceeds

**Files to modify:**
- `src/reconcile/job.rs` - add timeouts, error recovery
- `src/reconcile/startup.rs` - comprehensive orphan detection
- `src/db/schema.sql` - add last_transition_time timestamp

**Testing:**
```bash
# Scenario 1: Kill shim mid-volume-creation
terraform apply & sleep 5 && pkill kube-shim
# Wait for Hetzner to create volume (may take 1-2 min)
kube-shim (restart)
# Should detect orphan volume, delete it after ~5 min
sleep 10 && curl -k https://localhost:6443/debug/scan-orphans

# Scenario 2: Kill shim mid-container
terraform apply & sleep 30 && pkill kube-shim
# Wait 10s then restart
# Should detect container status, proceed to cleanup

# Repeat 10+ times, verify zero orphans remain
```

---

### Phase 8: Events + Metrics APIs (Days 8-9)
**Goal:** Add observability for `kubectl describe` and monitoring.

**Deliverables:**
- Events table in SQLite: reason, message, timestamp
- Emit events during reconciliation: "VolumeCreated", "VMStarting", "ContainerRunning", etc.
- Metrics API: `GET /apis/metrics.k8s.io/v1beta1/nodes` and `/pods`
- Estimate CPU/memory from running jobs (CPU cores * job count, etc.)

**Files to create/modify:**
- `src/api/events.rs` (new) - list events
- `src/api/metrics.rs` (new) - node and pod metrics
- `src/reconcile/job.rs` - emit events at each state change
- `src/db/schema.sql` - add events table

**Testing:**
```bash
kubectl describe pod osmdiffs-weekly-...
# Should see Events section with VolumeCreated, VMStarting, etc.

kubectl top nodes
kubectl top pods
# Should show estimated CPU/memory usage
```

---

### Phase 9: Pricing + Cost Tracking (Days 9-10)
**Goal:** Track Hetzner pricing, calculate job costs, expose via API.

**Deliverables:**
- Daily sync of Hetzner pricing API → SQLite cache table
- Cost calculation: (VM hourly price * duration) + (volume price per GB-hour * GB * duration)
- Estimated cost before job starts (optional)
- Actual cost calculated when job completes
- Expose via metrics API and events
- Hardcoded fallback pricing if API sync fails

**Files to create/modify:**
- `src/pricing/mod.rs` (new) - pricing sync and calculation
- `src/pricing/hetzner.rs` (new) - fetch Hetzner pricing API
- `src/api/pricing.rs` (new) - cost endpoint
- `src/reconcile/pricing.rs` (new) - background sync loop
- `src/db/schema.sql` - add hetzner_pricing table, estimated_cost_eur column

**Testing:**
```bash
# Check daily pricing sync
journalctl -u kube-shim -f | grep "pricing"
# Should see "Updated pricing" once per day

# After job completes
sqlite3 db.sqlite "SELECT name, estimated_cost_eur FROM jobs"
# Should show ~€0.50-1.00 per job

# Via API
curl -k https://localhost:6443/debug/status | jq '.total_cost_eur'
```

---

### Phase 10: Resource Naming + Cleanup (Days 10-11)
**Goal:** Identify resources created by shim, enable manual cleanup.

**Deliverables:**
- Naming convention: `kube-shim-{type}-{job-name}-{timestamp}-{random}`
- Manual cleanup script: deletes orphaned resources older than 7 days
- Orphan scan runs periodically (every 5 min in reconcile loop)
- Database tracks resource names for easy lookup

**Files to create/modify:**
- `src/naming.rs` (new) - resource name generation
- `bootstrap/cleanup-orphans.sh` (new) - manual cleanup script
- `src/reconcile/orphan_scan.rs` - improve to use names

**Testing:**
```bash
# Verify all Hetzner resources follow naming convention
curl -H "Authorization: Bearer $TOKEN" https://api.hetzner.cloud/v1/volumes | jq '.volumes[].name'
# All should start with "kube-shim-"

# Manual cleanup script
./cleanup-orphans.sh
# Should delete volumes/VMs older than 7 days
```

---

### Phase 11: Testing + Hardening (Days 11-12)
**Goal:** Run actual osmdiffs job, verify end-to-end, monitor for 1 week.

**Deliverables:**
- Swap 1GB test volume for 250GB
- Swap busybox for real osmdiffs image
- One full job run (6+ hours)
- Monitor Hetzner console, logs, costs
- Run 2-3 scheduled runs (wait for Sundays or trigger manually)

**Testing:**
```bash
# Deploy real osmdiffs CronJob
terraform apply

# Monitor:
journalctl -u kube-shim -f
curl -k https://localhost:6443/debug/status (every hour)
# Hetzner console (watch volume count)

# After job completes
kubectl logs osmdiffs-weekly-XXXXX
kubectl describe pod osmdiffs-weekly-XXXXX
curl -k https://localhost:6443/debug/status | jq '.total_cost_eur'
```

---

## Critical Files Summary

| File | Purpose | Status |
|------|---------|--------|
| `Cargo.toml` | Rust dependencies | Create (Phase 1) |
| `src/main.rs` | Server entry point | Create (Phase 1) |
| `src/api/*.rs` | Kubernetes API handlers | Create (Phases 1-8) |
| `src/db/schema.sql` | SQLite schema | Create (Phase 1) |
| `src/reconcile/*.rs` | State machine loop | Create (Phases 2-10) |
| `src/hetzner/*.rs` | Cloud API client | Create (Phases 3-5) |
| `src/pricing/*.rs` | Cost tracking | Create (Phase 9) |
| `src/config.rs` | Configuration parsing | Create (Phase 1) |
| `config.toml` | Runtime config template | Create (Phase 1) |
| `bootstrap/provision.sh` | One-time VPS setup | Create (Phase 1) |
| `bootstrap/cloud-init-template.sh` | VM startup script | Create (Phase 5) |
| `bootstrap/cleanup-orphans.sh` | Manual cleanup script | Create (Phase 10) |

---

## Key Implementation Details

### Dependencies (Cargo.toml)
```toml
axum = "0.7"
tokio = { version = "1", features = ["full"] }
sqlx = { version = "0.8", features = ["sqlite", "runtime-tokio"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
k8s-openapi = "0.22"
hcloud = "0.11"
tracing = "0.1"
tracing-subscriber = "0.3"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1"
config = "0.14"
reqwest = { version = "0.12", features = ["json"] }
```

### Reconciliation Loop State Machine
```
Created → VolumePending → VolumeCreating → VolumeCreated → VolumeAttaching
  → VolumeAttached → VMPending → VMCreating → VMRunning
  → ContainerRunning → Succeeded/Failed → VolumeDetaching
  → VolumeDeleted → Archived
```

Each state has:
- Entry conditions (can we transition?)
- Action (what to do?)
- Exit condition (when to move to next state?)
- Timeout (how long before failing?)
- Retry logic (backoff on failure)

### Database Strategy
- SQLite: single file, ACID, no external dependency
- Versioning: update rows by incrementing `version` column
- Reconciliation: poll every 10s for jobs not in terminal state
- State is single source of truth (SQlite is persistent across shim restarts)

### Hetzner Integration
- Use `hcloud` crate for type safety
- Dry-run mode for testing (logs API calls, doesn't execute)
- API token from config file (not K8s Secret, due to bootstrap problem)
- Resource naming for identification and cleanup

---

## Verification Strategy

### Per-Phase Checklist
- Phase 1: terraform apply/destroy works, resources stored in SQLite
- Phase 2: reconciliation loop advances job states automatically
- Phase 3: logs show "DRY-RUN" messages, no actual resources created
- Phase 4: small volumes created/deleted cleanly, orphan scan finds/deletes strays
- Phase 5: VMs launch, receive cloud-init, containers run
- Phase 6: kubectl logs -f works while container running
- Phase 7: 10+ chaos scenarios (kill shim, container crashes, etc.) with zero orphans
- Phase 8: kubectl describe shows events, kubectl top shows metrics
- Phase 9: costs calculated per job, total cost tracked
- Phase 10: all resources named consistently, cleanup script works
- Phase 11: real osmdiffs job completes successfully, cost accurate

### End-to-End Test
```bash
# 1. Provision VPS with provision.sh
# 2. Deploy shim (cargo build --release, copy to VPS, systemd service)
# 3. Apply Terraform config with real osmdiffs CronJob
# 4. Monitor until job completes (6+ hours)
# 5. Verify:
#    - kubectl logs shows full osmdiffs output
#    - kubectl get pod shows Succeeded status
#    - Hetzner console: volume created, then deleted
#    - Cost calculated: €0.50-1.50 per job
#    - Shim logs: no errors, all state transitions clean
# 6. Run 2-3 more jobs (over 1-2 weeks) with zero manual intervention
```

---

## Open Questions / Decisions

1. **Infomaniak vs. Hetzner**: Plan assumes Hetzner; Infomaniak requires OpenStack API (more complex). Initially target Hetzner, add Infomaniak later if needed.

2. **Container registry auth**: Assume public image (ghcr.io/brawer/osmdiffs public). If private, need to handle registry credentials in cloud-init.

3. **Terraform state**: Where does user store it? Assumed locally or in git (not critical to shim implementation).

4. **Monitoring**: Plan includes logging to journalctl. Optional: export to syslog/Loki later.

5. **RBAC/AuthN**: Assumed single-user trusted setup. Skip RBAC entirely.

---

## Timeline Estimate

- **Phase 1-2** (HTTP + reconciliation skeleton): 2 days
- **Phase 3-4** (Hetzner volumes): 2 days
- **Phase 5-6** (VMs + container + logs): 2 days
- **Phase 7-8** (hardening + events/metrics): 2 days
- **Phase 9-10** (pricing + naming): 2 days
- **Phase 11** (real workload testing): 2 days
- **Total**: ~2 weeks of development, 1+ week of running/monitoring

---
