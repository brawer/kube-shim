# Kubernetes API Shim for Ephemeral Container Workloads

## Context

**Problem:** Run a family of expensive, short-lived containerized batch workloads (starting with osmdiffs: 6 CPU, 8GB RAM, 250GB ephemeral disk, ~6hr runtime weekly, plus several similar scheduled cronjobs) on a budget VPS without paying for always-on infrastructure or managed Kubernetes node pools.

**Solution:** Build a lightweight Kubernetes API server that runs on a cheap VPS ($10-20/month) and:
- Accepts Kubernetes `Secret`, `PersistentVolumeClaim`, and `CronJob` manifests via Terraform
- Creates ephemeral Hetzner Cloud VMs per job run, sized per-job from the CronJob pod template's resource requests (not a single hardcoded size)
- Manages Hetzner Cloud block volumes provisioned via PersistentVolumeClaims — ephemeral (deleted after each run) by default, or retained across runs when a job wants to reuse cached data (e.g. a downloaded OSM planet file)
- Runs containers via cloud-init + Docker inside VMs
- Talks to the cloud through a small provider-agnostic interface — Hetzner Cloud is the only implementation initially, with Infomaniak (OpenStack) plannable as a second provider later without reworking the reconciliation loop (see "Future Work")
- Exposes Kubernetes-compatible APIs so Terraform `kubernetes_provider` works natively
- Streams logs, metrics, and cost tracking via custom Kubernetes APIs
- Serves a simple, publicly-reachable, read-only status page (port 80) with recent events, currently running jobs, and accumulated cost — no VPN or `kubectl` needed just to check in
- **Long-term, not initial scope:** also manage long-running `Deployment` workloads, reusing the same underlying VM-provisioning and volume-binding infrastructure built for CronJobs (see "Future Work" below)

**Key constraints:**
- Absolute requirement: large ephemeral scratch storage per job (up to 250GB for osmdiffs; other jobs in the family may need less)
- Multiple distinct cronjobs, potentially running concurrently — resource sizing, naming, and cost tracking must be per-job, not hardcoded to one workload
- Cheap indie developer infrastructure (total ~€50-60/month across the whole cronjob family)
- Must survive control plane restarts (crash recovery via reconciliation loop)
- Must prevent resource leaks (volumes, VMs) if any component fails
- Must prevent runaway cost if multiple jobs misfire concurrently or a single job runs long — enforced via a concurrency cap and a rolling (not daily-reset) budget guard, so the daily rate can be low while still allowing an occasional bigger job funded by unspent prior days
- The public status page has no authentication, so it must never render secrets, credentials, or anything else that isn't meant to be public

---

## Architecture Overview

### Components

**1. Control Plane (Shim):** Rust binary running on a cheap VPS
   - Axum HTTP server (port 6443, TLS) for the authenticated Kubernetes API
   - A second, unauthenticated Axum router (port 80, plain HTTP) serving only the read-only public status page — no mutating routes are ever registered on this listener
   - SQLite for persistent state (jobs, volumes, VMs, events, pricing, budget balance)
   - Reconciliation loop (10-second ticks) for state management
   - Cloud provider client (Hetzner first) behind a common interface, + pricing sync

**2. Worker VMs:** Short-lived Hetzner Cloud VMs (created per job run)
   - Receive volume attachment + container spec via cloud-init
   - Run Docker container with mounted volume
   - Communicate status/logs back to shim via SSH + status files

**3. Kubernetes API Surface:** Minimal implementation
   - Resources: `Secret` (v1), `PersistentVolumeClaim` (v1), `CronJob` (batch/v1), Pod status
   - Operations: CRUD on resources, watch/streaming, list
   - Extensions: Events, Metrics, Cost tracking (custom APIs)
   - **Future:** `Deployment` (apps/v1) for long-running workloads — deliberately deferred, but the internal `WorkloadKind` abstraction introduced in Phase 2 is designed so adding it later doesn't require reworking the CronJob path.

**4. Public Status Page:** static-ish HTML, served from port 80
   - Read-only, no auth, no `kubectl`/VPN required
   - Recent events, currently running jobs ("nodes"/pods), accumulated cost and budget balance

### Data Flow

```
Terraform (terraform apply)
  → HTTPS POST to Shim:6443 (kubernetes_provider)
    → Shim stores Secret / PersistentVolumeClaim / CronJob in SQLite
    → Reconciliation loop detects new jobs
      → Checks the rolling budget balance; if the job's estimated cost
        exceeds it, the job waits in BudgetWait until enough has accrued
      → Resolves the PersistentVolumeClaim referenced by the CronJob's pod
        template
        → New Hetzner volume if the PVC is Pending (or reuse the existing
          Hetzner volume if the PVC is Bound and its reclaim policy is
          Retain)
      → Launches a VM via the cloud provider interface, with cloud-init,
        sized from the pod template's resource requests
      → Cloud-init mounts volume, pulls image, docker run
      → Container writes to /scratch, streams logs
      → On exit: Shim detects completion, fetches logs, deletes VM
        (and the volume too, unless the PVC's reclaim policy is Retain),
        decrements the budget balance by the job's actual cost
    → Shim updates job status in SQLite
    → Terraform reads back status via GET requests
  → kubectl logs -f osmdiffs-weekly-...  (streams from VM via SSH)
  → kubectl get pod, kubectl describe, etc. (reads from Shim database)
  → kubectl get cronjobs  (lists the whole job family, not just osmdiffs)
  → Browser: GET http://<vps-ip>/  (public status page — events, running
    jobs, accumulated cost; read-only, no auth)
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
- Config: `max_concurrent_jobs`, plus the rolling-budget parameters `budget_daily_rate_eur` and `budget_rollover_cap_days` — this just reserves the config shape; the accrual/enforcement logic itself is built in Phase 10, once cost calculation exists.
- Internal `WorkloadKind` enum (`CronJob` for now) threaded through the reconciliation types, so the Phase 6+ VM-provisioning and PVC-binding code isn't written in a way that assumes "CronJob" is the only possible workload kind. This is purely an internal abstraction — no new API surface — done now so the future Deployment support (see "Future Work") doesn't require rewriting this layer.
- **Admission check on CronJob create/update: `spec.jobTemplate.spec.activeDeadlineSeconds` must be set.** `activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim needs a hard worst-case runtime bound for every job to make the budget guard (Phase 10) and deadline enforcement (Phase 8) meaningful, so it requires it via policy the same way a real cluster's `ValidatingAdmissionPolicy`/webhook would. A CronJob submitted without it is rejected with the same response shape a real admission webhook denial produces: HTTP 403, a `Status` object (`kind: Status`, `reason: Forbidden`), message `admission webhook "kube-shim.io/require-active-deadline" denied the request: spec.jobTemplate.spec.activeDeadlineSeconds must be set (bounds the job's worst-case cost against the budget guard)`. `kubectl`/Terraform surface this exactly like any real admission denial — resolves Open Question 9.

**Files to create/modify:**
- `src/api/pvc.rs` (new) - PersistentVolumeClaim CRUD
- `src/api/mod.rs` - register PVC routes, discovery entry
- `src/api/cronjob.rs` - add the `activeDeadlineSeconds` admission check on create/update
- `src/k8s_status.rs` (new) - builds Kubernetes-shaped `Status` error responses (reusable for any future admission/validation rejection, not just this one)
- `src/admission.rs` (new) - the `activeDeadlineSeconds` policy check itself
- `src/db/schema.sql` - add `persistent_volume_claims` table
- `src/config.rs` - add `max_concurrent_jobs`, `budget_daily_rate_eur`, `budget_rollover_cap_days`
- `src/workload.rs` (new) - `WorkloadKind` enum + shared resource-sizing helpers

**Testing:**
```bash
terraform apply   # now includes a PersistentVolumeClaim alongside Secret/CronJob
kubectl get pvc osmdiffs-scratch
sqlite3 db.sqlite "SELECT name, size_gb, reclaim_policy, status FROM persistent_volume_claims"
# Apply a second, differently-sized cronjob + PVC (e.g. a smaller weekly job)
# and confirm both are stored independently with no naming collisions.

# Admission check:
# Apply a CronJob with no activeDeadlineSeconds set
kubectl apply -f cronjob-no-deadline.yaml
# Error from server (Forbidden): error when creating "cronjob-no-deadline.yaml":
# admission webhook "kube-shim.io/require-active-deadline" denied the request: ...
terraform apply   # same manifest via Terraform: apply fails with the same message, cleanly
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

### Phase 4: Cloud Provider Integration (Dry-Run Mode) (Days 4-5)
**Goal:** Call the real Hetzner API but don't actually create resources yet — and define the provider interface so Hetzner isn't hardcoded throughout the codebase.

**Deliverables:**
- A `CloudProvider` trait (`create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `get_pricing`, ...) defined *before* writing any Hetzner-specific code. `HetznerProvider` is the only implementation for now. This is what lets Infomaniak (OpenStack) be added later as a second implementation instead of a rewrite (see "Future Work") — but it's a lightweight seam, not a finished multi-cloud abstraction; expect its exact method signatures to need adjustment once a second provider is actually implemented against it.
- Hetzner client setup (hcloud crate) implementing `CloudProvider`
- `DRY_RUN=true` config flag
- Reconciliation step: `VolumePending` → attempt volume creation (logged, not executed)
- Reconciliation step: `VMPending` → attempt VM creation (logged, not executed)
- Error handling for Hetzner API authentication failures

**Files to modify:**
- `src/providers/mod.rs` (new) - `CloudProvider` trait
- `src/providers/hetzner/mod.rs` (new) - Hetzner client wrapper implementing the trait
- `src/providers/hetzner/volumes.rs` (new) - volume operations
- `src/providers/hetzner/servers.rs` (new) - server operations
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
- `src/providers/hetzner/volumes.rs` - implement real create/attach/detach/delete
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
- `src/providers/hetzner/servers.rs` - implement real server create/delete
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
- Timeouts: volume creation (5 min), VM boot (5 min)
- **`activeDeadlineSeconds` enforcement (hard, not advisory):** since Phase 2's admission check guarantees every job has one set, the reconciliation loop tracks each running job's deadline and force-kills its VM (delete, not just stop the container) the moment it's exceeded — mirroring real Kubernetes Job behavior, where a Job that outlives `activeDeadlineSeconds` is terminated and marked `Failed` with reason `DeadlineExceeded`. This is what makes Phase 10's cost estimate an actual worst-case bound rather than a hopeful guess: nothing can silently run (and bill) past the deadline it declared at submission time.
- Cleanup on failure: unmount, detach, delete (even if one step fails) — including the deadline-exceeded case above
- Startup recovery: detect crashed containers, orphaned volumes, incomplete jobs, and jobs whose deadline passed while the shim itself was down
- Chaos testing scenario: kill shim mid-job, restart, verify cleanup proceeds

**Files to modify:**
- `src/reconcile/job.rs` - add timeouts, error recovery, label-based idempotency checks, `activeDeadlineSeconds` tracking + force-kill
- `src/reconcile/startup.rs` - comprehensive orphan detection, including deadline-exceeded jobs missed while down
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

# Scenario 3: deadline enforcement
# Apply a job with activeDeadlineSeconds=60 running a container that never exits
terraform apply
sleep 90
kubectl get pod osmdiffs-weekly-test
# Should show Failed, reason DeadlineExceeded; VM should be gone in Hetzner console
# Repeat once while killing the shim at t=30s, to confirm startup recovery also
# catches an expired deadline it missed while down
```

---

### Phase 9: Events + Metrics APIs (Days 9-10)
**Goal:** Add observability for `kubectl describe` and monitoring across the whole cronjob family, not just one workload.

**Deliverables:**
- Events table in SQLite: reason, message, timestamp
- Emit events during reconciliation: "VolumeCreated", "VMStarting", "ContainerRunning", "BudgetWait", etc.
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

### Phase 10: Pricing + Cost Tracking + Budget Guard (Days 10-11)
**Goal:** Track pricing, calculate job costs, and enforce a *rolling* budget guard — one that smooths a low daily rate across several days so an occasional bigger job can still run, rather than resetting to zero every day.

**Deliverables:**
- Daily sync of Hetzner pricing API → SQLite cache table (via the `CloudProvider::get_pricing()` method from Phase 4)
- Cost calculation: (VM hourly price × duration) + (volume price per GB-hour × GB × duration)
- Estimated cost before a job starts — now required, not optional, since the budget guard depends on it: hourly VM price × the job's `activeDeadlineSeconds` (its worst-case runtime) + estimated volume cost for that same duration. This is a true worst-case bound, not a hopeful guess: Phase 2's admission check guarantees every job has `activeDeadlineSeconds` set, and Phase 8 actually force-kills the VM if it's exceeded, so nothing can run past the duration this estimate assumes.
- Actual cost calculated when job completes
- Cost aggregated per job type and across the whole family (daily/weekly/monthly totals)
- **Rolling budget guard**, replacing a flat daily cap:
  - `budget_daily_rate_eur` (e.g. €2/day, from Phase 2's config) accrues into a persisted balance continuously, capped at `budget_daily_rate_eur × budget_rollover_cap_days` (default 7 days → €14 cap) — so several quiet days build up enough headroom for one bigger job, without the balance growing unbounded if jobs never run
  - Before launching a job, its estimated cost must be ≤ the current balance. If not, the job enters a new `BudgetWait` state and is retried every reconciliation tick — not failed — until enough balance has accrued. This resolves Open Question 7's "hard stop vs. soft alert" in favor of a hard stop.
  - The balance decrements by the job's *actual* cost once it completes (not the conservative estimate), so a job that finishes early returns its unused margin to the balance for the next one.
  - New `budget_state` table: a single row with `balance_eur`, `last_accrual_at`.
- Expose cost and budget balance via the metrics API, events, and the public status page (Phase 11)
- Hardcoded fallback pricing if the API sync fails

**Files to create/modify:**
- `src/providers/hetzner/pricing.rs` (new) - Hetzner pricing API fetch, implements `CloudProvider::get_pricing()`
- `src/pricing/mod.rs` (new) - provider-agnostic cost calculation + budget accrual/enforcement
- `src/api/pricing.rs` (new) - cost + budget-balance endpoint
- `src/reconcile/pricing.rs` (new) - background pricing sync loop + budget accrual tick + `BudgetWait` check
- `src/db/schema.sql` - add `provider_pricing` table, `estimated_cost_eur` column, `budget_state` table

**Testing:**
```bash
# Check daily pricing sync
journalctl -u kube-shim -f | grep "pricing"
# Should see "Updated pricing" once per day

# After job completes
sqlite3 db.sqlite "SELECT name, estimated_cost_eur FROM jobs"
# Should show ~€0.50-1.00 per job

# Via API
curl -k https://localhost:6443/debug/status | jq '.total_cost_eur, .budget_balance_eur'

# Rolling budget test:
# Set budget_daily_rate_eur=2, budget_rollover_cap_days=7
# Let 3 days pass with zero jobs running (or fast-forward budget_state.last_accrual_at for testing)
# Confirm balance ~= €6
# Apply a job estimated at €5 — should launch (balance covers it)
# Apply a second job estimated at €5 in the same tick — should enter BudgetWait
#   (only ~€1 left), then launch once enough balance has accrued
```

---

### Phase 11: Public Status Page (Days 11-12)
**Goal:** A simple, always-reachable, read-only page — no VPN or `kubectl` needed — showing recent events, currently running jobs, and accumulated cost.

**Deliverables:**
- A second Axum router, bound to `0.0.0.0:80` over plain HTTP, entirely separate from the authenticated `:6443` API. Only GET routes are ever registered on this router — by construction, not just by an auth check — so a bug elsewhere can't accidentally expose a write path on the public port.
- Single server-rendered HTML page, auto-refreshing (`<meta http-equiv="refresh">` or a few lines of polling JS), showing:
  - Recent events (Phase 9)
  - Currently running jobs ("nodes"/pods): name, job type, elapsed time, VM size
  - Accumulated cost and current budget balance / rollover cap (Phase 10)
- An explicit allowlist of what's rendered — job names, timestamps, event reasons/messages, cost figures. Secret values, S3 credentials, SSH details, and worker VM IPs must never appear here, since this listener has no authentication at all.
- `bootstrap/provision.sh` / systemd unit: grant `CAP_NET_BIND_SERVICE` (via systemd's `AmbientCapabilities=`) so the shim can bind port 80 without needing to run fully as root just for this.

**Files to create/modify:**
- `src/status_page.rs` (new) - renders the HTML page from DB reads (events, jobs, pricing/budget)
- `src/main.rs` - start the second listener on `:80` with its own router
- `bootstrap/provision.sh` - grant `CAP_NET_BIND_SERVICE`; update the systemd unit template

**Testing:**
```bash
curl http://<vps-ip>/
# From a phone browser: http://<vps-ip>/
# Confirm no Secret values, tokens, or SSH details appear anywhere in the page source
# Confirm POST/PUT/DELETE to port 80 all 404 (no such routes exist on this router)
```

---

### Phase 12: Resource Naming + Cleanup (Days 12-13)
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

### Phase 13: Testing + Hardening (Days 13-14)
**Goal:** Run real workloads from the cronjob family end-to-end, verify end-to-end, monitor for 1 week.

**Deliverables:**
- Swap 1GB test volume for the real per-job sizes (up to 250GB for osmdiffs)
- Swap busybox for real container images, across at least osmdiffs and one other cronjob from the family
- Run osmdiffs and at least one other cronjob concurrently at least once, to exercise the multi-job concurrency cap and budget guard under real conditions
- One full osmdiffs job run (6+ hours)
- Monitor Hetzner console, logs, costs, and the public status page
- Run 2-3 scheduled runs across the family (wait for real schedule triggers or trigger manually)

**Testing:**
```bash
# Deploy the real CronJobs (osmdiffs + at least one more from the family)
terraform apply

# Monitor:
journalctl -u kube-shim -f
curl -k https://localhost:6443/debug/status (every hour)
curl http://<vps-ip>/   # public status page, from your phone
# Hetzner console (watch volume + VM count across all jobs)

# After jobs complete
kubectl get cronjobs
kubectl logs osmdiffs-weekly-XXXXX
kubectl describe pod osmdiffs-weekly-XXXXX
curl -k https://localhost:6443/debug/status | jq '.total_cost_eur, .budget_balance_eur'
```

---

## Critical Files Summary

| File | Purpose | Status |
|------|---------|--------|
| `Cargo.toml` | Rust dependencies | Done (Phase 1) |
| `src/main.rs` | Server entry point, both listeners (`:6443` and `:80`) | Phases 1, 11 |
| `src/api/*.rs` | Kubernetes API handlers | Phases 1-2, 9 |
| `src/api/pvc.rs` | PersistentVolumeClaim CRUD | Create (Phase 2) |
| `src/workload.rs` | `WorkloadKind` abstraction (CronJob now, Deployment later) | Create (Phase 2) |
| `src/k8s_status.rs` | Kubernetes-shaped `Status` error responses (reused by any admission/validation rejection) | Create (Phase 2) |
| `src/admission.rs` | `activeDeadlineSeconds`-required policy check | Create (Phase 2) |
| `src/db/schema.sql` | SQLite schema | Phases 1-3, 5-6, 8-10 |
| `src/reconcile/*.rs` | State machine loop, incl. `activeDeadlineSeconds` enforcement (Phase 8) | Create (Phases 3, 5-6, 8, 10, 12) |
| `src/providers/*.rs` | `CloudProvider` trait + Hetzner implementation | Create (Phases 4-6, 10) |
| `src/pricing/*.rs` | Cost tracking + rolling budget guard | Create (Phase 10) |
| `src/status_page.rs` | Public read-only status page (port 80) | Create (Phase 11) |
| `src/config.rs` | Configuration parsing | Phases 1-2 |
| `config.toml` | Runtime config template | Create (Phase 1) |
| `bootstrap/provision.sh` | One-time VPS setup | Phases 1, 11 |
| `bootstrap/cloud-init-template.sh` | VM startup script | Create (Phase 6) |
| `bootstrap/cleanup-orphans.sh` | Manual cleanup script | Create (Phase 12) |

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

A job also passes through `BudgetWait` before `VolumePending` if its estimated cost exceeds the current rolling budget balance (Phase 10); it's retried each tick, not failed, once enough balance has accrued.

A running job that exceeds its `activeDeadlineSeconds` (Phase 8) is force-killed and transitions to `Failed` with reason `DeadlineExceeded` — the same terminology real Kubernetes Jobs use for this exact situation.

### Admission Validation
`activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim requires it on every CronJob (Phase 2) so the budget guard and deadline enforcement above have something to work with. Rather than silently defaulting it or inventing a bespoke error, a missing deadline is rejected the same way a real cluster's admission webhook/policy would reject a policy violation: HTTP 403, a standard `Status` object (`reason: Forbidden`), and a message in the same shape a tool like Gatekeeper or Kyverno would produce. `src/k8s_status.rs` builds this response generically, so any future admission/validation rule (not just this one) can reuse it.

### Workload Abstraction (for future Deployment support)
The reconciliation and VM-provisioning code is written against a `WorkloadKind` enum (Phase 2) rather than assuming "CronJob" directly. Today it has one variant. This costs nothing now but means that when `Deployment` support is added later (see "Future Work"), the shared plumbing — cloud provider client, PVC binding, cloud-init templating, log streaming, events, pricing — doesn't need to be reworked; only the state machine for "how a run starts/ends" differs per kind.

### Cloud Provider Abstraction (for future multi-provider support)
All cloud calls go through a `CloudProvider` trait (Phase 4) — `create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `get_pricing`, etc. `HetznerProvider` is the only implementation initially. This is a lightweight seam, not a finished multi-cloud abstraction: its exact method signatures should be expected to change once a second provider (see "Future Work: Additional VPS Providers") is actually implemented against it — Infomaniak's OpenStack API has a materially different shape (Keystone token auth instead of a static API key, Cinder volumes, Nova server "flavors", per-project quotas), and it's not worth guessing that shape correctly in advance.
- Dry-run mode for testing (logs API calls, doesn't execute)
- API token/credentials from config file (not K8s Secret, due to bootstrap problem)
- Resource naming for identification and cleanup

### Rolling Budget Model
A token bucket, not a fixed daily reset: `balance_eur` increases by `budget_daily_rate_eur` for every day (fractionally, per reconciliation tick) that passes, capped at `budget_daily_rate_eur × budget_rollover_cap_days`. Spending decrements the balance; a job that would exceed it waits in `BudgetWait` instead of being launched. Example: at €2/day with a 7-day cap, 3 idle days accrue €6 of balance — enough for one job estimated at €5, even though no single day's rate alone would cover it.

### Database Strategy
- SQLite: single file, ACID, no external dependency
- Versioning: update rows by incrementing `version` column
- Reconciliation: poll every 10s for jobs not in terminal state
- State is single source of truth (SQlite is persistent across shim restarts)

---

## Verification Strategy

### Per-Phase Checklist
- Phase 1: terraform apply/destroy works, resources stored in SQLite
- Phase 2: PVC CRUD works, CronJob resolves a referenced PVC, a second differently-sized cronjob coexists without naming collisions; a CronJob without `activeDeadlineSeconds` is rejected with a standard Kubernetes 403 `Status` response, surfaced cleanly by both `kubectl` and Terraform
- Phase 3: reconciliation loop advances job states automatically
- Phase 4: `CloudProvider` trait exists and `HetznerProvider` is the only caller of it (no direct hcloud calls elsewhere); logs show "DRY-RUN" messages, no actual resources created
- Phase 5: small volumes created/deleted cleanly, orphan scan finds/deletes strays without touching Retain-policy PVCs
- Phase 6: VMs launch sized per-job, receive cloud-init, containers run
- Phase 7: kubectl logs -f works while container running
- Phase 8: 10+ chaos scenarios (kill shim, container crashes, etc.) with zero orphans and zero duplicate resources; a job that overruns `activeDeadlineSeconds` is force-killed and marked `Failed`/`DeadlineExceeded`, including when the shim was down at the moment the deadline passed
- Phase 9: kubectl describe shows events, kubectl top shows metrics across all running jobs
- Phase 10: costs calculated per job and per family; a job whose estimate exceeds the budget balance waits in `BudgetWait` and launches once enough has accrued; a job that finishes early returns its unused margin to the balance
- Phase 11: status page reachable with no auth on port 80, shows events/jobs/cost, never leaks secrets, and rejects all non-GET requests
- Phase 12: all resources named consistently, cleanup script works
- Phase 13: real osmdiffs job and at least one other cronjob complete successfully, concurrently at least once, cost accurate

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
#    - Public status page (port 80) matches the authenticated debug/status output
#    - Shim logs: no errors, all state transitions clean
# 6. Run 2-3 more scheduled cycles (over 1-2 weeks) with zero manual intervention
```

---

## Open Questions / Decisions

1. **Infomaniak vs. Hetzner**: resolved for the initial scope — build against the `CloudProvider` trait (Phase 4) with `HetznerProvider` as the only implementation, and defer Infomaniak (OpenStack) to "Future Work" below rather than guessing its shape now.

2. **Container registry auth**: Assume public image (ghcr.io/brawer/osmdiffs public). If private, need to handle registry credentials in cloud-init.

3. **Terraform state**: Where does user store it? Assumed locally or in git (not critical to shim implementation).

4. **Monitoring**: Plan includes logging to journalctl, plus the new public status page. Still no notification path for a failed unattended run — worth deciding whether that's a log-only/status-page concern or needs an actual alert (email/webhook) before Phase 13.

5. **RBAC/AuthN**: Assumed single-user trusted setup. Skip RBAC entirely on the authenticated `:6443` API, but it currently ships with no auth token at all over plain HTTP — this must stay bound to localhost/VPN until at least a bearer token exists, since anyone reaching port 6443 could create jobs that spend the Hetzner budget. (The new port-80 status page is deliberately different: public and read-only by design, never able to mutate state — see Phase 11.)

6. **PVC default reclaim policy**: Phase 2 defaults to `Delete` (matches the original ephemeral-per-job-run design). Confirm this is the right default for every job in the family, or whether some jobs should default to `Retain` for caching.

7. **Concurrency guard scope**: Is `max_concurrent_jobs` a global cap across the whole family, or per-job-type? (The budget side of this question is now resolved — see the rolling budget guard in Phase 10: a hard stop via `BudgetWait`, not a soft alert.)

8. **Deployment networking (future)**: long-running Deployments will likely need a stable public IP/DNS name, unlike ephemeral CronJob VMs that are torn down after each run. Not needed for the initial scope, but worth deciding before "Future Work" below begins.

9. **Estimated cost accuracy**: resolved. Rather than guessing a default when `activeDeadlineSeconds` is absent, Phase 2 requires it via an admission check (HTTP 403, standard Kubernetes `Status` object, same shape a real admission-policy denial would produce) — so every job always has an explicit, user-chosen worst-case runtime, and Phase 8 force-kills the VM if it's exceeded. No default to get wrong, and the cost estimate is a real bound rather than a hopeful guess.

---

## Future Work: Deployments (`apps/v1`)

Not in the initial scope, but the design above is meant to make this additive rather than a rewrite:

- Add `Deployment` (apps/v1) as a second `WorkloadKind` (see Phase 2), alongside `CronJob`.
- Reuses as-is: the cloud provider client, PVC binding logic, cloud-init templating, log streaming, events/metrics, and pricing — all built for CronJobs.
- New pieces needed: a long-running VM state (create → run → restart-on-crash indefinitely, instead of create → run → delete once), a health-check/restart policy, and — per Open Question 8 — a stable network identity if the deployment needs to be reachable.
- Sequencing: start this only after the CronJob path (Phases 1-13) has run in production for a while and the reconciliation loop has proven itself reliable across restarts. A long-running workload has a much bigger blast radius for a reconciliation-loop bug than a 6-hour job does — a stuck CronJob run wastes at most one job's worth of money; a stuck Deployment could run (and bill) indefinitely.

---

## Future Work: Additional VPS Providers (Infomaniak / OpenStack)

Not in the initial scope. Hetzner covers the stated budget and constraints on its own; this is here so the `CloudProvider` seam (Phase 4) isn't accidentally designed in a way that forecloses it:

- Add an `OpenStackProvider` (or `InfomaniakProvider`, if Infomaniak's API diverges enough from stock OpenStack to need its own logic) implementing the same `CloudProvider` trait as `HetznerProvider`.
- Materially different shape from Hetzner: Keystone token-based auth (tokens expire and need refreshing, unlike Hetzner's static API key), Cinder for volumes, Nova for servers ("flavors" instead of server types, plus availability zones and per-project quotas to account for).
- Config would need a `provider: hetzner | infomaniak` selector and a per-provider credentials section, rather than the current single implicit Hetzner config block.
- Sequencing and caveat: don't start this until there's an actual need (e.g. Hetzner capacity/pricing/region no longer fits). Because the `CloudProvider` trait currently has exactly one implementation, its method signatures are a guess, not a validated abstraction — expect to revise the trait itself, not just add a new file, when this work actually begins.

---

## Timeline Estimate

- **Phase 1** (HTTP + CRUD scaffold): 2 days — done
- **Phase 2** (PVC API + multi-job resource model): 1 day
- **Phase 3-4** (reconciliation skeleton + provider trait + dry-run Hetzner): 2 days
- **Phase 5-6** (Hetzner volumes + VMs/containers): 2 days
- **Phase 7-8** (log streaming + hardening): 2 days
- **Phase 9-10** (events/metrics + pricing/rolling budget guard): 2 days
- **Phase 11** (public status page): 1 day
- **Phase 12** (naming + cleanup): 1 day
- **Phase 13** (multi-job real-workload testing): 2 days
- **Total**: ~3 weeks of development (up from ~2.5 weeks — the extra day is the new status-page phase), 1+ week of running/monitoring across the whole cronjob family. `Deployment` support and additional cloud providers (both under "Future Work") are intentionally excluded from this estimate.

---
