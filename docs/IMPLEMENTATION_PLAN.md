# Kubernetes API Shim for Ephemeral Container Workloads

## Context

**Problem:** Run a family of expensive, short-lived containerized batch workloads (starting with osmdiffs: 6 CPU, 8GB RAM, 250GB ephemeral disk, ~6hr runtime weekly, plus several similar scheduled cronjobs) on a budget VPS without paying for always-on infrastructure or managed Kubernetes node pools.

**Solution:** Build a lightweight Kubernetes API server that runs on a cheap VPS ($10-20/month) and:
- Accepts Kubernetes `Secret`, `PersistentVolumeClaim`, and `CronJob` manifests via Terraform, authenticated by a bearer token — no VPN or special networking required
- Creates ephemeral Hetzner Cloud VMs per job run, sized per-job from the CronJob pod template's resource requests (not a single hardcoded size)
- Manages Hetzner Cloud block volumes provisioned via PersistentVolumeClaims, using the standard `spec.storageClassName` field — ephemeral (deleted after each run) by default, or retained across runs when a job wants to reuse cached data (e.g. a downloaded OSM planet file)
- Runs containers via cloud-init + Docker inside VMs, on worker VMs that are unreachable from the internet (inbound) while still able to reach it (outbound) — via a Hetzner Cloud Firewall, not private networking
- Talks to the cloud through a small provider-agnostic interface — Hetzner Cloud is the only implementation initially, with Infomaniak (OpenStack) plannable as a second provider later without reworking the reconciliation loop (see "Future Work")
- Exposes Kubernetes-compatible APIs so Terraform `kubernetes_provider` works natively
- Streams logs, metrics, and cost tracking via custom Kubernetes APIs
- Serves a simple, publicly-reachable, read-only status page (port 8080) with recent events, currently running jobs, and accumulated cost — no VPN or `kubectl` needed just to check in
- Ships as a `FROM scratch` OCI container image, built in CI and run rootless under podman, so deploying and updating the shim itself is a `podman pull` + restart
- **Long-term, not initial scope:** also manage long-running `Deployment` workloads, reusing the same underlying VM-provisioning and volume-binding infrastructure built for CronJobs (see "Future Work" below)

**Key constraints:**
- Absolute requirement: large ephemeral scratch storage per job (up to 250GB for osmdiffs; other jobs in the family may need less)
- Multiple distinct cronjobs, potentially running concurrently — resource sizing, naming, and cost tracking must be per-job, not hardcoded to one workload
- Cheap indie developer infrastructure (total ~€50-60/month across the whole cronjob family)
- Must survive control plane restarts (crash recovery via reconciliation loop) — including a restart caused by updating the shim's own container image
- Must prevent resource leaks (volumes, VMs) if any component fails
- Must prevent runaway cost if a single job runs long or several run at once — enforced by a rolling (not daily-reset) budget guard alone, so the daily rate can be low while still allowing an occasional bigger job funded by unspent prior days; no separate job-count cap, since at the budget levels this project runs at, the budget itself already prevents meaningful concurrency in practice
- The authenticated `:6443` API must not be usable by anyone who doesn't hold a valid bearer token, since it can create jobs that spend the Hetzner budget — and this has to work without a VPN or other special networking
- Worker VMs must be unreachable from the internet (no inbound connections from anyone but the shim itself), while still able to reach the internet outbound (downloading input data, uploading results)
- The public status page has no authentication, so it must never render secrets, credentials, or anything else that isn't meant to be public

---

## Architecture Overview

### Components

**1. Control Plane (Shim):** Rust binary running on a cheap VPS, as a rootless podman container
   - Axum HTTP server (port 6443, TLS) for the authenticated Kubernetes API, wrapped in bearer-token auth middleware
   - A second, unauthenticated Axum router (port 8080, plain HTTP) serving only the read-only public status page — no mutating routes are ever registered on this listener
   - SQLite for persistent state (jobs, volumes, VMs, events, pricing, budget balance), stored on a bind-mounted host directory so it outlives container restarts/updates
   - Reconciliation loop (10-second ticks) for state management
   - Cloud provider client (Hetzner first) behind a common interface, + pricing sync

**2. Worker VMs:** Short-lived Hetzner Cloud VMs (created per job run)
   - Receive volume attachment + container spec via cloud-init
   - Run Docker container with mounted volume
   - Communicate status/logs back to shim via SSH (a native Rust client, not a subprocess) + status files
   - Protected by a Hetzner Cloud Firewall: no inbound from anywhere except the shim's own VPS IP, all outbound allowed — unreachable from the internet without needing private networking or a NAT gateway

**3. Kubernetes API Surface:** Minimal implementation
   - Resources: `Secret` (v1), `PersistentVolumeClaim` (v1), `CronJob` (batch/v1), Pod status
   - Operations: CRUD on resources, watch/streaming, list
   - Extensions: Events, Metrics, Cost tracking (custom APIs)
   - **Future:** `Deployment` (apps/v1) for long-running workloads — deliberately deferred, but the internal `WorkloadKind` abstraction introduced in Phase 4 is designed so adding it later doesn't require reworking the CronJob path.

**4. Public Status Page:** static-ish HTML, served from port 8080
   - Read-only, no auth, no `kubectl`/VPN required
   - Recent events, currently running jobs ("nodes"/pods), accumulated cost and budget balance

### Data Flow

```
Terraform (terraform apply, with a bearer token configured)
  → HTTPS POST to Shim:6443 (kubernetes_provider), auth middleware checks
    the bearer token before anything else runs
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
  → Browser: GET http://<vps-ip>:8080/  (public status page — events,
    running jobs, accumulated cost; read-only, no auth)

Separately: git tag vX.Y.Z && git push --tags
  → GitHub Actions builds a static musl binary, publishes
    ghcr.io/brawer/kube-shim:vX.Y.Z (FROM scratch, public, no pull auth)
  → Operator SSHes to the VPS, bumps the pinned tag in the podman quadlet
    unit, `podman pull` + `systemctl --user restart kube-shim`
  → SQLite state and worker VMs are unaffected by the restart
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

### Phase 2: Bearer Token Authentication (Days 2-3) — ✅ Complete
**Goal:** Close the gap this plan flagged in Open Question 5 — Phase 1 shipped with zero authentication on `:6443`. Since there's no VPN and no other special networking planned, this has to be solved with an in-band credential before the shim can safely face the internet.

**Deliverables:**
- A **list** of shared-secret bearer tokens rather than a single one — `api_tokens` in `config.toml`, each entry a generated token (e.g. via `openssl rand -base64 32`, never a human-chosen password) plus an optional `expires_at` (RFC 3339). This is what makes rotation possible: add the new token, update Terraform/kubectl to use it, then remove (or let expire) the old one — an overlap window rather than a single atomic cutover. It mirrors Kubernetes' own built-in "static token file" authenticator (`--token-auth-file`), which is itself a *list* of valid tokens for exactly this reason — not a bespoke design.
- Middleware wrapping the *entire* `:6443` router — every route, reads included, since Secret contents must never be visible without auth — that checks `Authorization: Bearer <token>` against every non-expired entry in the list, using a **constant-time comparison** for each (iterating the whole list unconditionally rather than short-circuiting on the first match, so total response time doesn't leak which entry — or whether any entry — matched).
- If the configured list is empty, or every entry has expired, **the shim refuses to start** rather than either locking the operator out confusingly at request time or — far worse — failing open and accepting all requests. This is checked once at startup and again whenever the config is reloaded.
- Missing or wrong token → HTTP 401 Unauthorized, using the standard Kubernetes `Status` object (`reason: Unauthorized`) — distinct from the 403 `Forbidden` used for admission denials (Phase 4): 401 means "I don't know who you are," 403 means "I know who you are and the answer is no," matching real API server semantics.
- The public status page (`:8080`, Phase 13) is deliberately *not* wrapped by this middleware — it stays unauthenticated by design.
- No custom client tooling needed: `kubectl config set-credentials ... --token=...` and Terraform's `kubernetes_provider` `token` argument both send exactly this header natively, regardless of how many tokens the shim currently considers valid.
- **From this phase onward, every `curl` example against `:6443` elsewhere in this document assumes `-H "Authorization: Bearer $TOKEN"` is included — omitted from later snippets for brevity, not because it's optional.**

**Files to create/modify:**
- `src/auth.rs` (new) - bearer-token middleware: list iteration, constant-time comparison per entry, expiration check
- `src/k8s_status.rs` (new) - builds Kubernetes-shaped `Status` error responses (401 here; reused by Phase 4's 403 admission check and any future validation rejection)
- `src/main.rs` - wrap the `:6443` router in the auth middleware; the `:8080` router (Phase 13) never gets it
- `src/config.rs` - add `api_tokens: Vec<{token, expires_at: Option<DateTime>}>`; refuse to boot if none currently valid
- `bootstrap/provision.sh` - generate the first token during first-time provisioning, print/save it once for the operator to copy into their kubeconfig/Terraform vars

**Testing:**
```bash
curl -k https://localhost:6443/api/v1
# 401 Unauthorized, Status object, reason Unauthorized

curl -k -H "Authorization: Bearer wrong-token" https://localhost:6443/api/v1
# 401 Unauthorized

curl -k -H "Authorization: Bearer $(cat api-token.txt)" https://localhost:6443/api/v1
# 200 OK, normal discovery response

# The public status page (once Phase 13 exists) must keep working with no auth at all:
curl http://localhost:8080/

# Rotation:
# Add a second token to api_tokens, restart, confirm BOTH the old and new
# token work simultaneously
# Set the old token's expires_at to the past, restart, confirm only the
# new token works and the old one now gets 401
# Empty the list entirely (or expire every entry) and confirm the shim
# refuses to start, rather than booting with no effective auth
```

Note: the token only protects the connection if TLS is actually used (already shipped in Phase 1) — a bearer token sent over plaintext HTTP is trivially sniffable, so this closes the loop only together with the existing TLS listener, never instead of it.

---

### Phase 3: Deploy & Update Mechanism (Container Image + Podman Quadlet) (Days 3-4) — ✅ Complete
**Goal:** A repeatable way to build, ship, and update the shim binary on the VPS: a `FROM scratch` OCI image built in CI, run rootless under podman, updated by pulling a specific pinned version — no VPN, no auto-updater, no privileged ports.

**Deliverables:**
- Rust binary built statically for `x86_64-unknown-linux-musl` (and `aarch64-unknown-linux-musl` — see multi-arch below) — no libc dependency at all, required for `FROM scratch` to even start the binary. `sqlx`'s SQLite `bundled` feature and `rustls` (already the plan's TLS choice over OpenSSL) are both static-friendly, so this doesn't force a dependency change.
- TLS root certificates embedded in the binary via `webpki-roots`, not `rustls-native-certs` — a scratch image has no `/etc/ssl/certs`, so outbound HTTPS to the Hetzner API needs its trust store compiled in.
- No shelling out to external binaries anywhere in the shim process: scratch has no shell and no `/usr/bin/anything` to exec. Phase 9's log streaming uses a pure-Rust SSH client library (`russh`) instead of invoking a system `ssh` binary. The same principle applies to any future subprocess-shaped need — reach for a Rust crate, not a bundled Linux distro.
- Runs as a non-root UID (1000) inside the container itself, on top of (not instead of) rootless podman on the host — defense in depth, and free, since the binary needs no special capabilities.
- **Multi-arch**: built for both `linux/amd64` and `linux/arm64` (via a GitHub Actions matrix, native builds on each architecture's own runner — no cross-compilation, `rust:1-alpine`'s default target already matches whichever arch it's running on), joined into a single multi-arch manifest. Revisits the original "amd64 only" plan: kept open for a possible future ARM VPS (e.g. Hetzner's CAX line) and consistency with this project's other repos, even though nothing currently requires it.
- GitHub Actions workflow: on a version tag push (now driven by `release-please`, not a manual `git tag` — see the release-please addendum below), builds and publishes `ghcr.io/brawer/kube-shim:vX.Y.Z` **and** `ghcr.io/brawer/kube-shim:latest`. The package is public, so the VPS needs no pull credentials at all.
  - **Revised from the original "never `:latest`" decision**: matching the convention used across this project's other repos (`:latest` alongside the immutable version tag, for convenience — e.g. quick manual testing) outweighed keeping kube-shim as a one-off exception. This doesn't change how the VPS itself is updated: `deploy/kube-shim.container` still pins an explicit `vX.Y.Z` tag, and updates there are still the deliberate manual action described below — `:latest` existing doesn't mean anything *uses* it for the real deployment.
  - **SLSA Build Level 3 provenance** is generated for every architecture-specific image and the joined manifest, via `actions/attest` (GitHub's native attestation action) run from a separate reusable workflow (`release-build.yml`) — not `slsa-framework/slsa-github-generator`, which an earlier version of this plan used; switched to match the same pattern already proven out in this project's sibling repos. The separate-workflow isolation (not a step folded into the same job that built the image) is what actually earns Level 3 rather than Level 1/2 either way — see `release-build.yml`'s own header comment.
  - A `verify-version` job cross-checks the pushed tag against `Cargo.toml`'s version before building anything, as a cheap sanity net (normally redundant, since `release-please` keeps them in sync itself, but cheap insurance against a stray manual tag).
- `bootstrap/provision.sh` rewritten around this: installs rootless podman; drops the earlier `scp` binary + hand-written systemd unit flow.
- A podman quadlet unit (`deploy/kube-shim.container`) defining: the image reference (pinned version), bind mounts for persistent state (`/var/lib/kube-shim` on the host → `/data` in the container — holds `db.sqlite`, the TLS certs, and `config.toml`), and port publishing for `:6443` and `:8080`. Both ports are >1024, so rootless podman binds them with no special capability or sysctl tweak — deliberately avoided by putting the status page on 8080 instead of 80.
- Updating is a **deliberate, manual action**, not an auto-updater: SSH in, bump the image tag in the quadlet unit, `podman pull` + `systemctl --user restart kube-shim`, watch `journalctl --user -u kube-shim -f`. Given the shim is trusted to spend real money orchestrating cloud resources, an unattended background update rolling out an untested release is a worse failure mode than a slightly stale binary.
- Because job/volume/VM/budget state lives entirely in SQLite (not in-process memory) and worker VMs run independently of the shim process, restarting the container for an update is safe even with jobs in flight — the reconciliation loop just resumes on its next tick, using the schema migrations that already run automatically on startup (Phase 1).

**Addendum: `release-please`.** Version tags are no longer pushed by hand — `release-please` (see `docs/RELEASING.md`) maintains a running release PR from Conventional-Commits PR titles and, once merged, tags the release itself, which is what actually triggers `release.yml`/`release-build.yml`. `Cargo.lock` is committed (it was gitignored at first, which broke the very first real release — see `docs/RELEASING.md` and PR history — a binary project needs a committed lock file for reproducible builds regardless, independent of that bug).

**Files created/modified:**
- `.github/workflows/release.yml` - thin entry point: tag-triggered, calls `release-build.yml` as a reusable workflow
- `.github/workflows/release-build.yml` (new) - the actual multi-arch build/manifest/attest pipeline (`verify-version` → `build` [matrix: amd64, arm64] → `manifest` → `attest`)
- `Containerfile` (was `Dockerfile` — renamed to match podman-first convention) - multi-stage: build stage compiles natively per-arch, final stage is `FROM scratch` + the binary, running as UID 1000
- `.dockerignore` - added, then removed again once the Containerfile switched to an explicit `COPY` allowlist (safer than a denylist that can fall out of sync)
- `bootstrap/provision.sh` - install podman; drop the old scp/systemd-unit deployment flow
- `deploy/kube-shim.container` (new) - podman quadlet unit: image ref, bind mounts, port publishing
- `Cargo.toml` / `Cargo.lock` - use `webpki-roots` (not `rustls-native-certs`); `Cargo.lock` committed, not gitignored

**Testing:**
```bash
# Cutting a release is now via release-please's PR, not a manual tag push --
# see docs/RELEASING.md. Once that PR is merged and the tag exists:

# On the VPS:
podman pull ghcr.io/brawer/kube-shim:v0.1.0
systemctl --user start kube-shim
journalctl --user -u kube-shim -f

# Update:
# bump the tag in deploy/kube-shim.container to the new version
podman pull ghcr.io/brawer/kube-shim:vX.Y.Z
systemctl --user restart kube-shim
# Confirm db.sqlite state (jobs, PVCs, budget balance) survived the restart
# Confirm an in-flight job's worker VM was untouched by the shim restart
```

---

### Phase 4: PersistentVolumeClaim API + Multi-Job Resource Model (Days 4-5)
**Goal:** Let Terraform manage volumes the same way it already manages Secrets and CronJobs, and remove the hardcoded single-workload assumptions from Phase 1 before the reconciliation loop is built on top of them.

**Deliverables:**
- `PersistentVolumeClaim` (v1) CRUD handlers: create/read/list/delete
  - `spec.resources.requests.storage`, `spec.accessModes`
  - Reclaim policy via the **standard** `spec.storageClassName` field, not a custom annotation: two built-in class names, `kube-shim-delete` (the default when `storageClassName` is omitted, matching the original ephemeral-per-run behavior) and `kube-shim-retain` (keeps the underlying Hetzner volume around between job runs — useful for a job that wants to cache a large downloaded dataset instead of re-fetching it weekly). No separate `StorageClass`/`PersistentVolume` CRUD is implemented — real dynamic provisioning's three-resource dance (StorageClass → PV → PVC) would be meaningfully more work for no practical benefit here, so the two class names are just a fixed, internally known lookup. An unknown `storageClassName` is rejected the standard way: HTTP 422, `Status` object, `reason: Invalid`, `causes: [{reason: FieldValueNotSupported, field: "spec.storageClassName"}]` — built via `src/k8s_status.rs` (Phase 2), same as every other structured error in this API.
- CronJob pod template can reference a PVC by name (`volumes: - persistentVolumeClaim: claimName: ...`); the reconciliation loop resolves it instead of always creating an ad-hoc volume per run.
- Database: `persistent_volume_claims` table (name, namespace, size_gb, access_mode, storage_class_name, bound_hetzner_volume_id, status: `Pending` / `Bound` / `Released`)
- Per-job VM sizing: read `resources.requests.cpu` / `.memory` from the CronJob's pod template and map to a Hetzner server type (small lookup table), instead of the fixed cx51 assumption — needed now that the shim runs more than one workload shape.
- Config: the rolling-budget parameters `budget_daily_rate_eur` and `budget_rollover_cap_days` — this just reserves the config shape; the accrual/enforcement logic itself is built in Phase 12, once cost calculation exists. No `max_concurrent_jobs` or other job-count cap: the budget guard is the only concurrency limiter (see Key Constraints) — at this project's spending levels it already rules out meaningful concurrent spend, so a second, separate cap would just be redundant bookkeeping.
- Internal `WorkloadKind` enum (`CronJob` for now) threaded through the reconciliation types, so the Phase 8+ VM-provisioning and PVC-binding code isn't written in a way that assumes "CronJob" is the only possible workload kind. This is purely an internal abstraction — no new API surface — done now so the future Deployment support (see "Future Work") doesn't require rewriting this layer.
- **Admission check on CronJob create/update: `spec.jobTemplate.spec.activeDeadlineSeconds` must be set.** `activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim needs a hard worst-case runtime bound for every job to make the budget guard (Phase 12) and deadline enforcement (Phase 10) meaningful, so it requires it via policy the same way a real cluster's `ValidatingAdmissionPolicy`/webhook would. A CronJob submitted without it is rejected with the same response shape a real admission webhook denial produces: HTTP 403, a `Status` object (`kind: Status`, `reason: Forbidden`, built via Phase 2's `src/k8s_status.rs`), message `admission webhook "kube-shim.io/require-active-deadline" denied the request: spec.jobTemplate.spec.activeDeadlineSeconds must be set (bounds the job's worst-case cost against the budget guard)`. `kubectl`/Terraform surface this exactly like any real admission denial — resolves Open Question 9.

**Files to create/modify:**
- `src/api/pvc.rs` (new) - PersistentVolumeClaim CRUD
- `src/api/mod.rs` - register PVC routes, discovery entry
- `src/api/cronjob.rs` - add the `activeDeadlineSeconds` admission check on create/update
- `src/admission.rs` (new) - the `activeDeadlineSeconds` policy check itself, reusing Phase 2's `src/k8s_status.rs` for the response
- `src/db/schema.sql` - add `persistent_volume_claims` table
- `src/config.rs` - add `budget_daily_rate_eur`, `budget_rollover_cap_days`
- `src/workload.rs` (new) - `WorkloadKind` enum + shared resource-sizing helpers

**Testing:**
```bash
terraform apply   # now includes a PersistentVolumeClaim alongside Secret/CronJob
kubectl get pvc osmdiffs-scratch
sqlite3 db.sqlite "SELECT name, size_gb, storage_class_name, status FROM persistent_volume_claims"
# Apply a second, differently-sized cronjob + PVC (e.g. a smaller weekly job)
# and confirm both are stored independently with no naming collisions.

# Reclaim policy:
kubectl apply -f pvc-with-unknown-storage-class.yaml
# Error from server (Invalid): ...spec.storageClassName: Unsupported value...

# Admission check:
# Apply a CronJob with no activeDeadlineSeconds set
kubectl apply -f cronjob-no-deadline.yaml
# Error from server (Forbidden): error when creating "cronjob-no-deadline.yaml":
# admission webhook "kube-shim.io/require-active-deadline" denied the request: ...
terraform apply   # same manifest via Terraform: apply fails with the same message, cleanly
```

---

### Phase 5: Reconciliation Loop Skeleton (Days 5-6)
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

### Phase 6: Cloud Provider Integration (Dry-Run Mode) (Days 6-7)
**Goal:** Call the real Hetzner API but don't actually create resources yet — and define the provider interface so Hetzner isn't hardcoded throughout the codebase.

**Deliverables:**
- A `CloudProvider` trait (`create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `create_firewall`, `get_pricing`, ...) defined *before* writing any Hetzner-specific code. `create_firewall` is what Phase 8 uses to lock worker VMs down to no inbound traffic except from the shim itself. `HetznerProvider` is the only implementation for now. This is what lets Infomaniak (OpenStack) be added later as a second implementation instead of a rewrite (see "Future Work") — but it's a lightweight seam, not a finished multi-cloud abstraction; expect its exact method signatures to need adjustment once a second provider is actually implemented against it.
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

### Phase 7: Real Hetzner Operations (Small Volumes) (Days 7-8)
**Goal:** Actually provision volumes, but small (1-10GB) for safety.

**Deliverables:**
- Set `dry_run = false` in config
- Real volume creation in reconciliation loop, resolved through the PVC binding added in Phase 4 (create a new Hetzner volume for a Pending PVC; reuse the existing one for a Bound PVC with reclaim policy Retain)
- Volume attachment to VPS
- Error handling + retry logic with timeouts (5 min for volume creation)
- Orphan detection: scan Hetzner for volumes with our prefix, delete if not tracked in DB — cross-check both `jobs.volume_id` and `persistent_volume_claims.bound_hetzner_volume_id` so a retained PVC's volume is never mistaken for an orphan
- Database: track volume_id, volume_device, mount_point

**Files to modify:**
- `src/providers/hetzner/volumes.rs` - implement real create/attach/detach/delete
- `src/reconcile/job.rs` - implement volume state steps (VolumeCreating, VolumeAttaching, VolumeAttached), including the PVC-resolution step from Phase 4
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

### Phase 8: VM Provisioning + Cloud-Init (Days 8-9)
**Goal:** Launch actual Hetzner Cloud VMs with containers inside.

**Deliverables:**
- Cloud-init script generation (bash, templated with proper escaping — job-supplied values such as image name/args/env must never be interpolated into shell unescaped; pass them as a base64-encoded blob decoded inside the VM instead)
- Real VM creation, sized per-job using the resource-request lookup table from Phase 4 (not a fixed cx51)
- **Every worker VM gets a Hetzner Cloud Firewall attached at creation time**: all inbound denied except SSH (22) from the shim's own VPS IP; all outbound allowed. This is what makes "unreachable from the internet, but can still reach it" true (Key Constraints) — no Hetzner private network, no NAT gateway, no extra routing needed; the VM keeps a normal public IP for outbound connectivity, it's just unreachable inbound from anyone but the shim.
- VM waits for volume attachment, mounts it
- VM pulls container image, runs docker
- Container write to /scratch
- Status file: `/tmp/job-status.txt` (written by cloud-init on completion)
- Container ID saved: `/tmp/container-id.txt`
- Database: track worker_vm_id, worker_vm_name, worker_ssh_ip

**Files to create/modify:**
- `src/providers/hetzner/servers.rs` - implement real server create/delete
- `src/providers/hetzner/firewall.rs` (new) - creates/attaches the restrictive Cloud Firewall for every worker VM
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

# Firewall:
# From a machine that is NOT the shim's VPS, confirm every port on the
# worker VM's public IP is unreachable (e.g. `nc -zv {worker-ip} 22` times out)
# From inside the worker VM (via the shim's own SSH access), confirm outbound
# still works, e.g. `curl -sI https://example.com` succeeds
```

---

### Phase 9: Log Streaming (Days 9-10)
**Goal:** Support `kubectl logs -f` for live monitoring.

**Deliverables:**
- HTTP endpoint: `GET /api/v1/namespaces/default/pods/{name}/log?follow=true`
- SSH to the worker VM using the `russh` client library (Phase 3) — never a subprocess — run `docker logs -f {container-id}` there, stream output back to the HTTP client
- Fallback: return cached logs if job is not running
- Handle SSH disconnects gracefully

**Files to create/modify:**
- `src/api/logs.rs` (new) - log streaming handler
- `src/api/mod.rs` - register logs endpoint
- `src/ssh.rs` (new) - SSH client built on `russh`, no subprocess

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

### Phase 10: Reconciliation Hardening (Days 10-11)
**Goal:** Make the reconciliation loop bulletproof for production.

**Deliverables:**
- Comprehensive error handling for all Hetzner API calls
- Idempotent state transitions (can retry without side effects) — before creating a volume or VM, check for an existing Hetzner resource carrying this job's name label, so a crash between "API call succeeded" and "DB write committed" can't create a duplicate on restart
- Timeouts: volume creation (5 min), VM boot (5 min)
- **`activeDeadlineSeconds` enforcement (hard, not advisory):** since Phase 4's admission check guarantees every job has one set, the reconciliation loop tracks each running job's deadline and force-kills its VM (delete, not just stop the container) the moment it's exceeded — mirroring real Kubernetes Job behavior, where a Job that outlives `activeDeadlineSeconds` is terminated and marked `Failed` with reason `DeadlineExceeded`. This is what makes Phase 12's cost estimate an actual worst-case bound rather than a hopeful guess: nothing can silently run (and bill) past the deadline it declared at submission time.
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

### Phase 11: Events + Metrics APIs (Days 11-12)
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

### Phase 12: Pricing + Cost Tracking + Budget Guard (Days 12-13)
**Goal:** Track pricing, calculate job costs, and enforce a *rolling* budget guard — one that smooths a low daily rate across several days so an occasional bigger job can still run, rather than resetting to zero every day.

**Deliverables:**
- Daily sync of Hetzner pricing API → SQLite cache table (via the `CloudProvider::get_pricing()` method from Phase 6)
- Cost calculation: (VM hourly price × duration) + (volume price per GB-hour × GB × duration)
- Estimated cost before a job starts — now required, not optional, since the budget guard depends on it: hourly VM price × the job's `activeDeadlineSeconds` (its worst-case runtime) + estimated volume cost for that same duration. This is a true worst-case bound, not a hopeful guess: Phase 4's admission check guarantees every job has `activeDeadlineSeconds` set, and Phase 10 actually force-kills the VM if it's exceeded, so nothing can run past the duration this estimate assumes.
- Actual cost calculated when job completes
- Cost aggregated per job type and across the whole family (daily/weekly/monthly totals)
- **Rolling budget guard**, replacing a flat daily cap:
  - `budget_daily_rate_eur` (e.g. €2/day, from Phase 4's config) accrues into a persisted balance continuously, capped at `budget_daily_rate_eur × budget_rollover_cap_days` (default 7 days → €14 cap) — so several quiet days build up enough headroom for one bigger job, without the balance growing unbounded if jobs never run
  - Before launching a job, its estimated cost must be ≤ the current balance. If not, the job enters a new `BudgetWait` state and is retried every reconciliation tick — not failed — until enough balance has accrued. This resolves Open Question 7's "hard stop vs. soft alert" in favor of a hard stop.
  - The balance decrements by the job's *actual* cost once it completes (not the conservative estimate), so a job that finishes early returns its unused margin to the balance for the next one.
  - New `budget_state` table: a single row with `balance_eur`, `last_accrual_at`.
- Expose cost and budget balance via the metrics API, events, and the public status page (Phase 13)
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

### Phase 13: Public Status Page (Days 13-14)
**Goal:** A simple, always-reachable, read-only page — no VPN or `kubectl` needed — showing recent events, currently running jobs, and accumulated cost.

**Deliverables:**
- A second Axum router, bound to `0.0.0.0:8080` over plain HTTP, entirely separate from the authenticated `:6443` API. Only GET routes are ever registered on this router — by construction, not just by an auth check — so a bug elsewhere can't accidentally expose a write path on the public port. Port 8080 (rather than 80) means rootless podman (Phase 3) can publish it with no special capability or sysctl tweak.
- Single server-rendered HTML page, auto-refreshing (`<meta http-equiv="refresh">` or a few lines of polling JS), showing:
  - Recent events (Phase 11)
  - Currently running jobs ("nodes"/pods): name, job type, elapsed time, VM size
  - Accumulated cost and current budget balance / rollover cap (Phase 12)
- An explicit allowlist of what's rendered — job names, timestamps, event reasons/messages, cost figures. Secret values, S3 credentials, SSH details, and worker VM IPs must never appear here, since this listener has no authentication at all.
- The `:8080` port-publish line is added to the podman quadlet unit (`deploy/kube-shim.container`, Phase 3).

**Files to create/modify:**
- `src/status_page.rs` (new) - renders the HTML page from DB reads (events, jobs, pricing/budget)
- `src/main.rs` - start the second listener on `:8080` with its own router
- `deploy/kube-shim.container` - publish `:8080`

**Testing:**
```bash
curl http://<vps-ip>:8080/
# From a phone browser: http://<vps-ip>:8080/
# Confirm no Secret values, tokens, or SSH details appear anywhere in the page source
# Confirm POST/PUT/DELETE to :8080 all 404 (no such routes exist on this router)
```

---

### Phase 14: Resource Naming + Cleanup (Days 14-15)
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

### Phase 15: Testing + Hardening (Days 15-16)
**Goal:** Run real workloads from the cronjob family end-to-end, verify end-to-end, monitor for 1 week.

**Deliverables:**
- Swap 1GB test volume for the real per-job sizes (up to 250GB for osmdiffs)
- Swap busybox for real container images, across at least osmdiffs and one other cronjob from the family
- Run osmdiffs and at least one other cronjob concurrently at least once, to exercise the budget guard under real concurrent-job conditions
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
curl http://<vps-ip>:8080/   # public status page, from your phone
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
| `Cargo.toml` | Rust dependencies | Phases 1, 3 |
| `src/main.rs` | Server entry point, both listeners (`:6443` and `:8080`) | Phases 1-3, 13 |
| `src/auth.rs` | Bearer-token authentication middleware for `:6443` | Create (Phase 2) |
| `src/k8s_status.rs` | Kubernetes-shaped `Status` error responses (401 here, reused for 403 admission denials) | Create (Phase 2) |
| `.github/workflows/release.yml` | CI: build musl binary, publish OCI image to ghcr.io | Create (Phase 3) |
| `Dockerfile` | Multi-stage build → `FROM scratch` image | Create (Phase 3) |
| `deploy/kube-shim.container` | Podman quadlet unit (image ref, bind mounts, ports) | Phases 3, 13 |
| `src/api/*.rs` | Kubernetes API handlers | Phases 1, 4, 11 |
| `src/api/pvc.rs` | PersistentVolumeClaim CRUD | Create (Phase 4) |
| `src/workload.rs` | `WorkloadKind` abstraction (CronJob now, Deployment later) | Create (Phase 4) |
| `src/admission.rs` | `activeDeadlineSeconds`-required policy check | Create (Phase 4) |
| `src/db/schema.sql` | SQLite schema | Phases 1, 4-5, 7-8, 10-12 |
| `src/reconcile/*.rs` | State machine loop, incl. `activeDeadlineSeconds` enforcement (Phase 10) | Create (Phases 5, 7-8, 10, 12) |
| `src/providers/*.rs` | `CloudProvider` trait + Hetzner implementation | Create (Phases 6-8, 12) |
| `src/providers/hetzner/firewall.rs` | Restrictive Cloud Firewall for worker VMs (inbound denied except from the shim) | Create (Phase 8) |
| `src/pricing/*.rs` | Cost tracking + rolling budget guard | Create (Phase 12) |
| `src/ssh.rs` | SSH client built on `russh` | Create (Phase 9) |
| `src/status_page.rs` | Public read-only status page (port 8080) | Create (Phase 13) |
| `src/config.rs` | Configuration parsing | Phases 1-2, 4 |
| `config.toml` | Runtime config template | Create (Phase 1) |
| `bootstrap/provision.sh` | One-time VPS setup | Phases 1-3 |
| `bootstrap/cloud-init-template.sh` | VM startup script | Create (Phase 8) |
| `bootstrap/cleanup-orphans.sh` | Manual cleanup script | Create (Phase 14) |

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
webpki-roots = "0.26"
russh = "0.45"
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

A job also passes through `BudgetWait` before `VolumePending` if its estimated cost exceeds the current rolling budget balance (Phase 12); it's retried each tick, not failed, once enough balance has accrued.

A running job that exceeds its `activeDeadlineSeconds` (Phase 10) is force-killed and transitions to `Failed` with reason `DeadlineExceeded` — the same terminology real Kubernetes Jobs use for this exact situation.

### Authentication
`:6443` requires `Authorization: Bearer <token>` on every request (Phase 2), checked against every non-expired entry in `api_tokens` with a constant-time comparison. A missing or wrong token gets a standard Kubernetes `Status` object with `reason: Unauthorized` (HTTP 401) — separate from the `reason: Forbidden` (HTTP 403) used for admission denials below, matching real API server conventions. `:8080` (Phase 13) is deliberately excluded from this middleware; it's meant to be public. Supporting a *list* of tokens (each with an optional expiry) rather than a single one is what makes rotation possible: add a new token, migrate clients, then remove the old one — never a single atomic cutover with no overlap window.

### Admission Validation
`activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim requires it on every CronJob (Phase 4) so the budget guard and deadline enforcement above have something to work with. Rather than silently defaulting it or inventing a bespoke error, a missing deadline is rejected the same way a real cluster's admission webhook/policy would reject a policy violation: HTTP 403, a standard `Status` object (`reason: Forbidden`), and a message in the same shape a tool like Gatekeeper or Kyverno would produce. `src/k8s_status.rs` (Phase 2) builds this response generically, so any future admission/validation rule (not just this one) can reuse it.

### Storage Classes / Reclaim Policy
Rather than a custom annotation, reclaim policy is exposed through the PVC's standard `spec.storageClassName` field (Phase 4) — one of two built-in, hardcoded class names (`kube-shim-delete`, the default, and `kube-shim-retain`). No real `StorageClass` or `PersistentVolume` resource type is implemented; that would mean modeling actual Kubernetes dynamic provisioning's three-resource dance for no practical benefit at this project's scale, so the two names are just an internally known lookup table, not a CRUD'd API resource. An unknown class name is rejected via `src/k8s_status.rs`, HTTP 422 `reason: Invalid` — the same idiomatic shape a real cluster uses for "this enum value isn't one of the supported ones," distinct from the 403 `Forbidden` used for the `activeDeadlineSeconds` policy check above (that's a cluster policy denying an otherwise-valid request; this is a plain field-value validation failure).

### Worker VM Network Isolation
Every worker VM (Phase 8) gets a Hetzner Cloud Firewall at creation time: all inbound denied except SSH from the shim's own VPS IP, all outbound allowed. This achieves "unreachable from the internet, but can still reach it" (Key Constraints) without Hetzner private networking or a NAT gateway — the VM keeps its normal public IP (needed for outbound egress), it's just unreachable inbound from anyone but the shim. Chosen over private networking specifically for simplicity: a NAT-gateway-via-the-control-plane setup would need extra routing/IP-forwarding configuration on both ends for a security benefit (never touching the public internet even for shim↔worker traffic) that's largely redundant once the firewall already blocks every other inbound source.

### Workload Abstraction (for future Deployment support)
The reconciliation and VM-provisioning code is written against a `WorkloadKind` enum (Phase 4) rather than assuming "CronJob" directly. Today it has one variant. This costs nothing now but means that when `Deployment` support is added later (see "Future Work"), the shared plumbing — cloud provider client, PVC binding, cloud-init templating, log streaming, events, pricing — doesn't need to be reworked; only the state machine for "how a run starts/ends" differs per kind.

### Cloud Provider Abstraction (for future multi-provider support)
All cloud calls go through a `CloudProvider` trait (Phase 6) — `create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `get_pricing`, etc. `HetznerProvider` is the only implementation initially. This is a lightweight seam, not a finished multi-cloud abstraction: its exact method signatures should be expected to change once a second provider (see "Future Work: Additional VPS Providers") is actually implemented against it — Infomaniak's OpenStack API has a materially different shape (Keystone token auth instead of a static API key, Cinder volumes, Nova server "flavors", per-project quotas), and it's not worth guessing that shape correctly in advance.
- Dry-run mode for testing (logs API calls, doesn't execute)
- API token/credentials from config file (not K8s Secret, due to bootstrap problem)
- Resource naming for identification and cleanup

### Rolling Budget Model
A token bucket, not a fixed daily reset: `balance_eur` increases by `budget_daily_rate_eur` for every day (fractionally, per reconciliation tick) that passes, capped at `budget_daily_rate_eur × budget_rollover_cap_days`. Spending decrements the balance; a job that would exceed it waits in `BudgetWait` instead of being launched. Example: at €2/day with a 7-day cap, 3 idle days accrue €6 of balance — enough for one job estimated at €5, even though no single day's rate alone would cover it.

### Deployment Model
The shim ships as a `FROM scratch` OCI image (Phase 3): a statically-linked musl binary with embedded TLS roots and no external process dependencies (SSH goes through `russh`, not a subprocess), run rootless via a podman quadlet unit. Persistent state (`db.sqlite`, TLS certs, `config.toml`) lives on a host bind mount, so a container restart — whether from a crash or a deliberate `podman pull` + `systemctl --user restart` for an update — never loses job/volume/VM/budget state, and any in-flight job's worker VM is unaffected since it runs independently on Hetzner. Updates are always a manual, deliberate action, never an unattended auto-updater, given the shim spends real money.

### Database Strategy
- SQLite: single file, ACID, no external dependency
- Versioning: update rows by incrementing `version` column
- Reconciliation: poll every 10s for jobs not in terminal state
- State is single source of truth (SQlite is persistent across shim restarts)

---

## Verification Strategy

### Per-Phase Checklist
- Phase 1: terraform apply/destroy works, resources stored in SQLite
- Phase 2: `:6443` rejects requests with no/wrong bearer token (401, standard `Status` object, constant-time comparison); multiple tokens can be valid simultaneously (rotation), expired tokens are rejected, and the shim refuses to start with zero valid tokens configured; the public status page (once it exists in Phase 13) stays unauthenticated
- Phase 3: `FROM scratch` image builds and starts (static musl binary, no libc); `podman pull` + `systemctl --user restart` picks up a new version with `db.sqlite` and in-flight jobs unaffected; ghcr.io package requires no pull credentials
- Phase 4: PVC CRUD works, CronJob resolves a referenced PVC, a second differently-sized cronjob coexists without naming collisions; an unknown `storageClassName` is rejected with a 422 `Status` response; a CronJob without `activeDeadlineSeconds` is rejected with a standard Kubernetes 403 `Status` response, surfaced cleanly by both `kubectl` and Terraform
- Phase 5: reconciliation loop advances job states automatically
- Phase 6: `CloudProvider` trait exists and `HetznerProvider` is the only caller of it (no direct hcloud calls elsewhere); logs show "DRY-RUN" messages, no actual resources created
- Phase 7: small volumes created/deleted cleanly, orphan scan finds/deletes strays without touching Retain-policy PVCs
- Phase 8: VMs launch sized per-job, receive cloud-init, containers run; every worker VM is unreachable inbound from outside the shim's own IP but can still reach the internet outbound
- Phase 9: kubectl logs -f works while container running, via `russh` (no `ssh` subprocess spawned by the shim)
- Phase 10: 10+ chaos scenarios (kill shim, container crashes, etc.) with zero orphans and zero duplicate resources; a job that overruns `activeDeadlineSeconds` is force-killed and marked `Failed`/`DeadlineExceeded`, including when the shim was down at the moment the deadline passed
- Phase 11: kubectl describe shows events, kubectl top shows metrics across all running jobs
- Phase 12: costs calculated per job and per family; a job whose estimate exceeds the budget balance waits in `BudgetWait` and launches once enough has accrued; a job that finishes early returns its unused margin to the balance
- Phase 13: status page reachable with no auth on port 8080, shows events/jobs/cost, never leaks secrets, and rejects all non-GET requests
- Phase 14: all resources named consistently, cleanup script works
- Phase 15: real osmdiffs job and at least one other cronjob complete successfully, concurrently at least once, cost accurate

### End-to-End Test
```bash
# 1. Provision VPS with provision.sh (installs rootless podman)
# 2. Deploy shim: podman pull the released image, start via the quadlet unit
# 3. Apply Terraform config with the real cronjob family (osmdiffs + others),
#    using the bearer token from provisioning
# 4. Monitor until jobs complete (6+ hours for osmdiffs)
# 5. Verify:
#    - kubectl logs shows full output for each job
#    - kubectl get pod shows Succeeded status
#    - Hetzner console: volumes created, then deleted (except Retain-policy ones)
#    - Cost calculated per job and aggregated across the family
#    - Public status page (port 8080) matches the authenticated debug/status output
#    - Shim logs: no errors, all state transitions clean
# 6. Run 2-3 more scheduled cycles (over 1-2 weeks) with zero manual intervention
# 7. Tag and ship one real update during this window: confirm state and
#    in-flight jobs survive the podman pull + restart
```

---

## Open Questions / Decisions

All ten questions below are now resolved or deliberately deferred — none block starting Phase 1's already-shipped work or any subsequent phase.

1. **Infomaniak vs. Hetzner**: resolved for the initial scope — build against the `CloudProvider` trait (Phase 6) with `HetznerProvider` as the only implementation, and defer Infomaniak (OpenStack) to "Future Work" below rather than guessing its shape now.

2. **Container registry auth for *workload* images**: resolved by inference — all of this project's ghcr.io packages are public (open-source work only), so `ghcr.io/brawer/osmdiffs` and the rest of the cronjob family's images are assumed public too, same as the shim's own image (Phase 3). If any workload image later turns out to be private, cloud-init will need registry credentials added — flag that specifically if it comes up, since it wasn't confirmed job-by-job.

3. **Terraform state**: resolved — out of scope for the shim regardless of how Terraform itself is run (locally, or as a GitHub Action). The shim is just an HTTPS server authenticated by a bearer token; where Terraform's own state file lives has no bearing on it.

4. **Monitoring / alerting**: deliberately deferred, not implemented in the initial scope. There's no single established generic webhook standard for arbitrary Kubernetes events — the closest ecosystem convention is Prometheus Alertmanager's webhook receiver (built on metrics + alerting rules) or ad hoc event-forwarders like `eventrouter`, both of which are downstream tooling the cluster operator adds, not something the API server defines. OpenTelemetry itself is a telemetry *data pipeline* (traces/metrics/logs), not an alerting system — alerting is a separate concern layered on top of an OTel-fed backend (Grafana, Prometheus, or a vendor). None of that infrastructure is warranted for a single-VPS personal project. If this gets built later, the lightest-weight fit is a simple outbound webhook POST (a JSON payload) on specific events — job `Failed`/`DeadlineExceeded`, budget balance running low — to a user-configured URL (a Slack incoming webhook, `ntfy.sh`, `healthchecks.io`, etc.), no Prometheus/OTel Collector required. For now: journalctl + the status page only.

5. **RBAC/AuthN**: resolved for the initial scope. Full RBAC is still out of scope (single-user trusted setup), but Phase 2 requires a bearer token — mirroring Kubernetes' own built-in static-token-file authenticator — on every request to `:6443`, closing the "must stay bound to VPN" gap without needing a VPN or any special networking. The public `:8080` status page remains deliberately unauthenticated by design (read-only, structurally unable to mutate state) — see Phase 13.

6. **PVC default reclaim policy**: resolved. Since implementing full standard Kubernetes dynamic provisioning (a real `StorageClass` + `PersistentVolume` resource pair) would be meaningfully more than a day's work for no practical benefit at this scale, the shim instead exposes reclaim policy through the PVC's *standard* `spec.storageClassName` field (a real, already-optional PVC field) mapped to two hardcoded, internally known class names — `kube-shim-delete` (default) and `kube-shim-retain` — rather than either a custom annotation or dropping the feature. See Phase 4 and the "Storage Classes / Reclaim Policy" section under Key Implementation Details.

7. **Concurrency guard scope**: resolved — dropped entirely, no separate `max_concurrent_jobs`. The rolling budget guard (Phase 12) is the only limiter: at the budget levels this project runs at, it already prevents more than one job's worth of concurrent spend in practice, so a second, separate job-count cap would just be redundant bookkeeping.

8. **Worker VM networking**: resolved for the initial scope. Every worker VM is unreachable from the internet inbound (a Hetzner Cloud Firewall permits nothing but SSH from the shim's own VPS IP) while remaining free to reach the internet outbound for downloading input data and uploading results — see Phase 8 and "Worker VM Network Isolation" under Key Implementation Details. Exposing any of this *inbound*, e.g. running the shim itself as a configurable reverse proxy driven by a Kubernetes `Ingress` resource, is explicitly out of scope for a long time — see "Future Work: Ingress Proxy" below.

9. **Estimated cost accuracy**: resolved. Rather than guessing a default when `activeDeadlineSeconds` is absent, Phase 4 requires it via an admission check (HTTP 403, standard Kubernetes `Status` object, same shape a real admission-policy denial would produce) — so every job always has an explicit, user-chosen worst-case runtime, and Phase 10 force-kills the VM if it's exceeded. No default to get wrong, and the cost estimate is a real bound rather than a hopeful guess.

10. **Bearer token rotation**: resolved. `api_tokens` (Phase 2) is a list, each entry with an optional expiration, checked against all of them — so a new token can be added and adopted by Terraform/kubectl before the old one is removed or allowed to expire, instead of a single atomic cutover with no overlap window. See "Authentication" under Key Implementation Details.

---

## Future Work: Deployments (`apps/v1`)

Not in the initial scope, but the design above is meant to make this additive rather than a rewrite:

- Add `Deployment` (apps/v1) as a second `WorkloadKind` (see Phase 4), alongside `CronJob`.
- Reuses as-is: the cloud provider client, PVC binding logic, cloud-init templating, log streaming, events/metrics, and pricing — all built for CronJobs.
- New pieces needed: a long-running VM state (create → run → restart-on-crash indefinitely, instead of create → run → delete once), a health-check/restart policy, and — per Open Question 8 — a stable network identity if the deployment needs to be reachable.
- Sequencing: start this only after the CronJob path (Phases 1-15) has run in production for a while and the reconciliation loop has proven itself reliable across restarts. A long-running workload has a much bigger blast radius for a reconciliation-loop bug than a 6-hour job does — a stuck CronJob run wastes at most one job's worth of money; a stuck Deployment could run (and bill) indefinitely.

---

## Future Work: Additional VPS Providers (Infomaniak / OpenStack)

Not in the initial scope. Hetzner covers the stated budget and constraints on its own; this is here so the `CloudProvider` seam (Phase 6) isn't accidentally designed in a way that forecloses it:

- Add an `OpenStackProvider` (or `InfomaniakProvider`, if Infomaniak's API diverges enough from stock OpenStack to need its own logic) implementing the same `CloudProvider` trait as `HetznerProvider`.
- Materially different shape from Hetzner: Keystone token-based auth (tokens expire and need refreshing, unlike Hetzner's static API key), Cinder for volumes, Nova for servers ("flavors" instead of server types, plus availability zones and per-project quotas to account for).
- Config would need a `provider: hetzner | infomaniak` selector and a per-provider credentials section, rather than the current single implicit Hetzner config block.
- Sequencing and caveat: don't start this until there's an actual need (e.g. Hetzner capacity/pricing/region no longer fits). Because the `CloudProvider` trait currently has exactly one implementation, its method signatures are a guess, not a validated abstraction — expect to revise the trait itself, not just add a new file, when this work actually begins.

---

## Future Work: kube-shim as an Ingress Proxy

Way out of scope — not sketched in any detail here, just a placeholder so the direction is recorded:

- The idea: let kube-shim itself act as a reverse proxy in front of select worker VMs (or, more likely, future Deployments — see "Future Work: Deployments" above), configured through a standard Kubernetes `Ingress` resource, so specific services could deliberately be made reachable from the internet.
- This directly reverses the network isolation model established in Phase 8 (worker VMs unreachable inbound by design) for whatever is explicitly exposed through it, so whenever this is actually pursued it needs its own security design pass, not an incremental bolt-on: TLS termination, which `Ingress` fields are even honored, and how it interacts with the existing Hetzner Cloud Firewall per worker VM.
- No `Ingress` (networking.k8s.io/v1) support exists anywhere in the current API surface (Architecture Overview) — this would be new API surface, not a variation on something already planned.

---

## Timeline Estimate

- **Phase 1** (HTTP + CRUD scaffold): 2 days — done
- **Phase 2** (bearer token authentication): 1 day
- **Phase 3** (deploy & update mechanism: image, CI, podman quadlet): 2 days
- **Phase 4** (PVC API + multi-job resource model + deadline admission check): 1 day
- **Phase 5-6** (reconciliation skeleton + provider trait + dry-run Hetzner): 2 days
- **Phase 7-8** (Hetzner volumes + VMs/containers): 2 days
- **Phase 9-10** (log streaming + hardening/deadline enforcement): 2 days
- **Phase 11-12** (events/metrics + pricing/rolling budget guard): 2 days
- **Phase 13** (public status page): 1 day
- **Phase 14** (naming + cleanup): 1 day
- **Phase 15** (multi-job real-workload testing): 2 days
- **Total**: ~3.5 weeks of development (up from ~3 weeks — the extra 2-3 days are the new auth and deploy/update phases), 1+ week of running/monitoring across the whole cronjob family. `Deployment` support and additional cloud providers (both under "Future Work") are intentionally excluded from this estimate.

---
