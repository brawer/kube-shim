# Kubernetes API Shim for Ephemeral Container Workloads

## Context

**Problem:** Run a family of expensive, short-lived containerized batch workloads (starting with osmdiffs: 6 CPU, 8GB RAM, 250GB ephemeral disk, ~6hr runtime weekly, plus several similar scheduled cronjobs) on a budget VPS without paying for always-on infrastructure or managed Kubernetes node pools.

**Solution:** Build a lightweight Kubernetes API server that runs on a cheap VPS ($10-20/month) and:
- Accepts Kubernetes `Secret`, `PersistentVolumeClaim`, and `CronJob` manifests via Terraform
- Creates ephemeral Hetzner Cloud VMs per job run, sized per-job from the CronJob pod template's resource requests (not a single hardcoded size)
- Manages Hetzner Cloud block volumes provisioned via PersistentVolumeClaims — ephemeral (deleted after each run) by default, or retained across runs when a job wants to reuse cached data (e.g. a downloaded OSM planet file)
- Runs containers via cloud-init + Docker inside VMs
- Exposes Kubernetes-compatible APIs so Terraform `kubernetes_provider` works natively
- Streams logs, metrics, and cost tracking via custom Kubernetes APIs
- **Long-term, not initial scope:** also manage long-running `Deployment` workloads, reusing the same underlying VM-provisioning and volume-binding infrastructure built for CronJobs (see "Future Work" below)

**Key constraints:**
- Absolute requirement: large ephemeral scratch storage per job (up to 250GB for osmdiffs; other jobs in the family may need less)
- Multiple distinct cronjobs, potentially running concurrently — resource sizing, naming, and cost tracking must be per-job, not hardcoded to one workload
- Cheap indie developer infrastructure (total ~€50-60/month across the whole cronjob family)
- Must survive control plane restarts (crash recovery via reconciliation loop)
- Must prevent resource leaks (volumes, VMs) if any component fails
- Must prevent runaway cost if multiple jobs misfire concurrently (concurrency cap / budget guard)

---

## Architecture Overview

### Components

**1. Control Plane (Shim):** Rust binary running on a cheap VPS
   - Axum HTTP server (port 6443, TLS)
   - SQLite for persistent state (jobs, volumes, VMs, events, pricing)
   - Reconciliation loop (10-second ticks) for state management
   - Hetzner Cloud API client + pricing sync

**2. Worker VMs:** Short-lived Hetzner Cloud VMs (created per job run)
   - Receive volume attachment + container spec via cloud-init
   - Run Docker container with mounted volume
   - Communicate status/logs back to shim via SSH + status files

**3. Kubernetes API Surface:** Minimal implementation
   - Resources: `Secret` (v1), `PersistentVolumeClaim` (v1), `CronJob` (batch/v1), Pod status
   - Operations: CRUD on resources, watch/streaming, list
   - Extensions: Events, Metrics, Cost tracking (custom APIs)
   - **Future:** `Deployment` (apps/v1) for long-running workloads — deliberately deferred, but the internal `WorkloadKind` abstraction introduced in Phase 2 is designed so adding it later doesn't require reworking the CronJob path.

### Data Flow

```
Terraform (terraform apply)
  → HTTPS POST to Shim:6443 (kubernetes_provider)
    → Shim stores Secret / PersistentVolumeClaim / CronJob in SQLite
    → Reconciliation loop detects new jobs
      → Resolves the PersistentVolumeClaim referenced by the CronJob's pod
        template
        → New Hetzner volume if the PVC is Pending (or reuse the existing
          Hetzner volume if the PVC is Bound and its reclaim policy is
          Retain)
      → Launches Hetzner VM with cloud-init, sized from the pod template's
        resource requests
      → Cloud-init mounts volume, pulls image, docker run
      → Container writes to /scratch, streams logs
      → On exit: Shim detects completion, fetches logs, deletes VM
        (and the volume too, unless the PVC's reclaim policy is Retain)
    → Shim updates job status in SQLite
    → Terraform reads back status via GET requests
  → kubectl logs -f osmdiffs-weekly-...  (streams from VM via SSH)
  → kubectl get pod, kubectl describe, etc. (reads from Shim database)
  → kubectl get cronjobs  (lists the whole job family, not just osmdiffs)
```

---

## Implementation Phases

### Phase 1: Scaffold + HTTP API (Days 1-2) — ✅ Complete
**Goal:** Prove Terraform can CRUD resources via the shim.

No changes from the original plan — this phase already shipped exactly as designed. See `docs/PHASE_1_COMPLETION.md` for the completion report.

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

### Phase 2: PersistentVolumeClaim API + Multi-Job Resource Model (Days 2-3)
**Goal:** Let Terraform manage volumes the same way it already manages Secrets and CronJobs, and remove the hardcoded single-workload assumptions from Phase 1 before the reconciliation loop is built on top of them.

**Deliverables:**
- `PersistentVolumeClaim` (v1) CRUD handlers: create/read/list/delete
  - `spec.resources.requests.storage`, `spec.accessModes`
  - Shim-specific annotation `kube-shim.io/reclaim-policy: Delete|Retain` (default `Delete`, matching the original ephemeral-per-run behavior). `Retain` keeps the underlying Hetzner volume around between job runs — useful for a job that wants to cache a large downloaded dataset instead of re-fetching it weekly.
- CronJob pod template can reference a PVC by name (`volumes: - persistentVolumeClaim: claimName: ...`); the reconciliation loop resolves it instead of always creating an ad-hoc volume per run.
- Database: `persistent_volume_claims` table (name, namespace, size_gb, access_mode, reclaim_policy, bound_hetzner_volume_id, status: `Pending` / `Bound` / `Released`)
- Per-job VM sizing: read `resources.requests.cpu` / `.memory` from the CronJob's pod template and map to a Hetzner server type (small lookup table), instead of the fixed cx51 assumption — needed now that the shim runs more than one workload shape.
- Config: `max_concurrent_jobs` and an optional `daily_budget_eur` circuit breaker in `config.toml`, so a misconfigured or duplicated cronjob can't silently rack up cost across the whole family.
- Internal `WorkloadKind` enum (`CronJob` for now) threaded through the reconciliation types, so the Phase 6+ VM-provisioning and PVC-binding code isn't written in a way that assumes "CronJob" is the only possible workload kind. This is purely an internal abstraction — no new API surface — done now so the future Deployment support (see "Future Work") doesn't require rewriting this layer.

**Files to create/modify:**
- `src/api/pvc.rs` (new) - PersistentVolumeClaim CRUD
- `src/api/mod.rs` - register PVC routes, discovery entry
- `src/db/schema.sql` - add `persistent_volume_claims` table; add `max_concurrent_jobs`/budget config
- `src/config.rs` - add `max_concurrent_jobs`, `daily_budget_eur`
- `src/workload.rs` (new) - `WorkloadKind` enum + shared resource-sizing helpers

**Testing:**
```bash
terraform apply   # now includes a PersistentVolumeClaim alongside Secret/CronJob
kubectl get pvc osmdiffs-scratch
sqlite3 db.sqlite "SELECT name, size_gb, reclaim_policy, status FROM persistent_volume_claims"
# Apply a second, differently-sized cronjob + PVC (e.g. a smaller weekly job)
# and confirm both are stored independently with no naming collisions.
```

---

### Phase 3: Reconciliation Loop Skeleton (Days 3-4)
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

### Phase 4: Hetzner Integration (Dry-Run Mode) (Days 4-5)
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

### Phase 5: Real Hetzner Operations (Small Volumes) (Days 5-6)
**Goal:** Actually provision volumes, but small (1-10GB) for safety.

**Deliverables:**
- Set `dry_run = false` in config
- Real volume creation in reconciliation loop, resolved through the PVC binding added in Phase 2 (create a new Hetzner volume for a Pending PVC; reuse the existing one for a Bound PVC with reclaim policy Retain)
- Volume attachment to VPS
- Error handling + retry logic with timeouts (5 min for volume creation)
- Orphan detection: scan Hetzner for volumes with our prefix, delete if not tracked in DB — cross-check both `jobs.volume_id` and `persistent_volume_claims.bound_hetzner_volume_id` so a retained PVC's volume is never mistaken for an orphan
- Database: track volume_id, volume_device, mount_point

**Files to modify:**
- `src/hetzner/volumes.rs` - implement real create/attach/detach/delete
- `src/reconcile/job.rs` - implement volume state steps (VolumeCreating, VolumeAttaching, VolumeAttached), including the PVC-resolution step from Phase 2
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
# Repeat once with a Retain-policy PVC and confirm the volume survives
# terraform destroy of the CronJob (but not the PVC)
```

**Exit criteria:**
- 10 cycles of create→delete volume with zero orphans
- Hetzner bill still near zero

---

### Phase 6: VM Provisioning + Cloud-Init (Days 6-7)
**Goal:** Launch actual Hetzner Cloud VMs with containers inside.

**Deliverables:**
- Cloud-init script generation (bash, templated with proper escaping — job-supplied values such as image name/args/env must never be interpolated into shell unescaped; pass them as a base64-encoded blob decoded inside the VM instead)
- Real VM creation, sized per-job using the resource-request lookup table from Phase 2 (not a fixed cx51)
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

### Phase 7: Log Streaming (Days 7-8)
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

### Phase 8: Reconciliation Hardening (Days 8-9)
**Goal:** Make the reconciliation loop bulletproof for production.

**Deliverables:**
- Comprehensive error handling for all Hetzner API calls
- Idempotent state transitions (can retry without side effects) — before creating a volume or VM, check for an existing Hetzner resource carrying this job's name label, so a crash between "API call succeeded" and "DB write committed" can't create a duplicate on restart
- Timeouts: volume creation (5 min), VM boot (5 min), container runtime (6 hours + job timeout)
- Cleanup on failure: unmount, detach, delete (even if one step fails)
- Startup recovery: detect crashed containers, orphaned volumes, incomplete jobs
- Chaos testing scenario: kill shim mid-job, restart, verify cleanup proceeds

**Files to modify:**
- `src/reconcile/job.rs` - add timeouts, error recovery, label-based idempotency checks
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

### Phase 9: Events + Metrics APIs (Days 9-10)
**Goal:** Add observability for `kubectl describe` and monitoring across the whole cronjob family, not just one workload.

**Deliverables:**
- Events table in SQLite: reason, message, timestamp
- Emit events during reconciliation: "VolumeCreated", "VMStarting", "ContainerRunning", etc.
- Metrics API: `GET /apis/metrics.k8s.io/v1beta1/nodes` and `/pods`
- Estimate CPU/memory from running jobs (CPU cores * job count, etc.), aggregated across every cronjob, not just osmdiffs

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
# Should show estimated CPU/memory usage across all concurrently running jobs
```

---

### Phase 10: Pricing + Cost Tracking (Days 10-11)
**Goal:** Track Hetzner pricing, calculate job costs, expose via API, and enforce the budget guard from Phase 2.

**Deliverables:**
- Daily sync of Hetzner pricing API → SQLite cache table
- Cost calculation: (VM hourly price * duration) + (volume price per GB-hour * GB * duration)
- Estimated cost before job starts (optional)
- Actual cost calculated when job completes
- Cost aggregated per job type and across the whole family (daily/monthly totals)
- Enforce `daily_budget_eur` from Phase 2: once exceeded, pause launching new job runs and emit an event/log line, rather than silently continuing
- Expose via metrics API and events
- Hardcoded fallback pricing if API sync fails

**Files to create/modify:**
- `src/pricing/mod.rs` (new) - pricing sync and calculation
- `src/pricing/hetzner.rs` (new) - fetch Hetzner pricing API
- `src/api/pricing.rs` (new) - cost endpoint
- `src/reconcile/pricing.rs` (new) - background sync loop + budget guard check
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

# Set daily_budget_eur artificially low, confirm new job launches are paused
# and an event is emitted once the budget is exceeded
```

---

### Phase 11: Resource Naming + Cleanup (Days 11-12)
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

### Phase 12: Testing + Hardening (Days 12-13)
**Goal:** Run real workloads from the cronjob family end-to-end, verify end-to-end, monitor for 1 week.

**Deliverables:**
- Swap 1GB test volume for the real per-job sizes (up to 250GB for osmdiffs)
- Swap busybox for real container images, across at least osmdiffs and one other cronjob from the family
- Run osmdiffs and at least one other cronjob concurrently at least once, to exercise the multi-job concurrency cap and budget guard under real conditions
- One full osmdiffs job run (6+ hours)
- Monitor Hetzner console, logs, costs
- Run 2-3 scheduled runs across the family (wait for real schedule triggers or trigger manually)

**Testing:**
```bash
# Deploy the real CronJobs (osmdiffs + at least one more from the family)
terraform apply

# Monitor:
journalctl -u kube-shim -f
curl -k https://localhost:6443/debug/status (every hour)
# Hetzner console (watch volume + VM count across all jobs)

# After jobs complete
kubectl get cronjobs
kubectl logs osmdiffs-weekly-XXXXX
kubectl describe pod osmdiffs-weekly-XXXXX
curl -k https://localhost:6443/debug/status | jq '.total_cost_eur'
```

---

## Critical Files Summary

| File | Purpose | Status |
|------|---------|--------|
| `Cargo.toml` | Rust dependencies | Done (Phase 1) |
| `src/main.rs` | Server entry point | Done (Phase 1) |
| `src/api/*.rs` | Kubernetes API handlers | Phases 1-2, 9 |
| `src/api/pvc.rs` | PersistentVolumeClaim CRUD | Create (Phase 2) |
| `src/workload.rs` | `WorkloadKind` abstraction (CronJob now, Deployment later) | Create (Phase 2) |
| `src/db/schema.sql` | SQLite schema | Phases 1-3, 5-6, 8-10 |
| `src/reconcile/*.rs` | State machine loop | Create (Phases 3, 5-6, 8, 10-11) |
| `src/hetzner/*.rs` | Cloud API client | Create (Phases 4-6) |
| `src/pricing/*.rs` | Cost tracking + budget guard | Create (Phase 10) |
| `src/config.rs` | Configuration parsing | Phases 1-2 |
| `config.toml` | Runtime config template | Create (Phase 1) |
| `bootstrap/provision.sh` | One-time VPS setup | Create (Phase 1) |
| `bootstrap/cloud-init-template.sh` | VM startup script | Create (Phase 6) |
| `bootstrap/cleanup-orphans.sh` | Manual cleanup script | Create (Phase 11) |

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

`VolumePending → VolumeCreating` additionally requires the job's referenced PersistentVolumeClaim to be resolvable: either it's `Pending` (create a fresh Hetzner volume, then mark the PVC `Bound`) or it's already `Bound` with reclaim policy `Retain` (reuse the existing Hetzner volume). A job whose PVC doesn't exist yet stays in `Created` rather than erroring, since Terraform may apply the PVC and the CronJob in the same run in either order.

### Workload Abstraction (for future Deployment support)
The reconciliation and VM-provisioning code is written against a `WorkloadKind` enum (Phase 2) rather than assuming "CronJob" directly. Today it has one variant. This costs nothing now but means that when `Deployment` support is added later (see "Future Work"), the shared plumbing — Hetzner client, PVC binding, cloud-init templating, log streaming, events, pricing — doesn't need to be reworked; only the state machine for "how a run starts/ends" differs per kind.

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
- Phase 2: PVC CRUD works, CronJob resolves a referenced PVC, a second differently-sized cronjob coexists without naming collisions
- Phase 3: reconciliation loop advances job states automatically
- Phase 4: logs show "DRY-RUN" messages, no actual resources created
- Phase 5: small volumes created/deleted cleanly, orphan scan finds/deletes strays without touching Retain-policy PVCs
- Phase 6: VMs launch sized per-job, receive cloud-init, containers run
- Phase 7: kubectl logs -f works while container running
- Phase 8: 10+ chaos scenarios (kill shim, container crashes, etc.) with zero orphans and zero duplicate resources
- Phase 9: kubectl describe shows events, kubectl top shows metrics across all running jobs
- Phase 10: costs calculated per job and per family, total cost tracked, budget guard pauses new launches when exceeded
- Phase 11: all resources named consistently, cleanup script works
- Phase 12: real osmdiffs job and at least one other cronjob complete successfully, concurrently at least once, cost accurate

### End-to-End Test
```bash
# 1. Provision VPS with provision.sh
# 2. Deploy shim (cargo build --release, copy to VPS, systemd service)
# 3. Apply Terraform config with the real cronjob family (osmdiffs + others)
# 4. Monitor until jobs complete (6+ hours for osmdiffs)
# 5. Verify:
#    - kubectl logs shows full output for each job
#    - kubectl get pod shows Succeeded status
#    - Hetzner console: volumes created, then deleted (except Retain-policy ones)
#    - Cost calculated per job and aggregated across the family
#    - Shim logs: no errors, all state transitions clean
# 6. Run 2-3 more scheduled cycles (over 1-2 weeks) with zero manual intervention
```

---

## Open Questions / Decisions

1. **Infomaniak vs. Hetzner**: Plan assumes Hetzner; Infomaniak requires OpenStack API (more complex). Initially target Hetzner, add Infomaniak later if needed.

2. **Container registry auth**: Assume public image (ghcr.io/brawer/osmdiffs public). If private, need to handle registry credentials in cloud-init.

3. **Terraform state**: Where does user store it? Assumed locally or in git (not critical to shim implementation).

4. **Monitoring**: Plan includes logging to journalctl. Optional: export to syslog/Loki later. Still no notification path for a failed unattended run — worth deciding whether that's a log-only concern or needs an actual alert (email/webhook) before Phase 12.

5. **RBAC/AuthN**: Assumed single-user trusted setup. Skip RBAC entirely, but Phase 1 currently ships with no auth token at all over plain HTTP — this must stay bound to localhost/VPN until at least a bearer token exists, since anyone reaching port 6443 could create jobs that spend the Hetzner budget.

6. **PVC default reclaim policy**: Phase 2 defaults to `Delete` (matches the original ephemeral-per-job-run design). Confirm this is the right default for every job in the family, or whether some jobs should default to `Retain` for caching.

7. **Concurrency/budget guard scope**: Is `max_concurrent_jobs` a global cap across the whole family, or per-job-type? Is `daily_budget_eur` a hard stop (refuse new job launches) or just an alert that still lets jobs run?

8. **Deployment networking (future)**: long-running Deployments will likely need a stable public IP/DNS name, unlike ephemeral CronJob VMs that are torn down after each run. Not needed for the initial scope, but worth deciding before "Future Work" below begins.

---

## Future Work: Deployments (`apps/v1`)

Not in the initial scope, but the design above is meant to make this additive rather than a rewrite:

- Add `Deployment` (apps/v1) as a second `WorkloadKind` (see Phase 2), alongside `CronJob`.
- Reuses as-is: the Hetzner client, PVC binding logic, cloud-init templating, log streaming, events/metrics, and pricing — all built for CronJobs.
- New pieces needed: a long-running VM state (create → run → restart-on-crash indefinitely, instead of create → run → delete once), a health-check/restart policy, and — per Open Question 8 — a stable network identity if the deployment needs to be reachable.
- Sequencing: start this only after the CronJob path (Phases 1-12) has run in production for a while and the reconciliation loop has proven itself reliable across restarts. A long-running workload has a much bigger blast radius for a reconciliation-loop bug than a 6-hour job does — a stuck CronJob run wastes at most one job's worth of money; a stuck Deployment could run (and bill) indefinitely.

---

## Timeline Estimate

- **Phase 1** (HTTP + CRUD scaffold): 2 days — done
- **Phase 2** (PVC API + multi-job resource model): 1 day
- **Phase 3-4** (reconciliation skeleton + dry-run Hetzner): 2 days
- **Phase 5-6** (Hetzner volumes + VMs/containers): 2 days
- **Phase 7-8** (log streaming + hardening): 2 days
- **Phase 9-10** (events/metrics + pricing/budget guard): 2 days
- **Phase 11** (naming + cleanup): 1 day
- **Phase 12** (multi-job real-workload testing): 2 days
- **Total**: ~2.5 weeks of development (the extra half-day over the original estimate is Phase 2's PVC/multi-job work), 1+ week of running/monitoring across the whole cronjob family. `Deployment` support (Future Work) is intentionally excluded from this estimate.

---
