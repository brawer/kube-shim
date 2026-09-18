# Kubernetes API Shim for Ephemeral Container Workloads

## Context

**Problem:** Run a family of expensive, short-lived containerized batch workloads (starting with osmdiffs: 6 CPU, 8GB RAM, 250GB ephemeral disk, ~6hr runtime weekly, plus several similar scheduled cronjobs) on a budget VPS without paying for always-on infrastructure or managed Kubernetes node pools.

**Solution:** Build a lightweight Kubernetes API server that runs on a cheap VPS (~€3-20/month) and:
- Accepts Kubernetes `Secret` and `CronJob` manifests via Terraform, authenticated by a bearer token — no VPN or special networking required
- Creates ephemeral UpCloud Cloud Server instances per job run, sized per-job from the CronJob pod template's resource requests (not a single hardcoded size)
- Provisions per-job scratch storage as a **generic ephemeral volume**, declared inline in the pod template (`volumes: - ephemeral: volumeClaimTemplate: ...`) the same way real Kubernetes does, rather than a standalone `PersistentVolumeClaim` resource — always created and deleted with the job run that owns it. `spec.storageClassName` selects a performance tier (`kube-shim-standard` default, `kube-shim-fast`), mapped internally to whichever cloud provider's closest matching storage tier
- Runs containers via cloud-init + rootless Podman inside VMs, on worker VMs that are unreachable from the internet (inbound) while still able to reach it (outbound) — via UpCloud firewall rules, not private networking
- Talks to the cloud through a small provider-agnostic interface — UpCloud is the only implementation initially, with Hetzner and Infomaniak (OpenStack) plannable as additional providers later without reworking the reconciliation loop (see "Future Work")
- Serves its own TLS certificate via ACME (Let's Encrypt), Caddy-like: point a real hostname at it and it issues and renews its own certificate automatically, no manual cert handling — falling back to a self-signed certificate when no public hostname is configured (local dev, CI)
- Exposes Kubernetes-compatible APIs so Terraform `kubernetes_provider` works natively
- Streams logs, metrics, and cost tracking via custom Kubernetes APIs, including a CSV cost report grouped by job labels
- Tracks and enforces a rolling budget in the operator's own currency of choice (`main_currency`), converting the cloud provider's EUR-denominated actual spend via the ECB's daily reference rates
- Serves a simple, publicly-reachable, read-only status page plus standard Kubernetes-style health endpoints (`/`, `/healthz`, `/livez`, `/readyz`) on the same `:443` listener as the authenticated API — reusing its ACME certificate rather than a separate plaintext port — with recent events, currently running jobs, and accumulated cost — no VPN or `kubectl` needed just to check in
- Ships as a `FROM scratch` OCI container image, built in CI and run rootless under podman, so deploying and updating the shim itself is a `podman pull` + restart (or, for the maintainer's own instance, fully automatic via `podman-auto-update.timer`)
- **Long-term, not initial scope:** also manage long-running `Deployment` workloads, and standalone durable `PersistentVolumeClaim`s for a job that wants to cache data across runs — both reuse the same underlying VM-provisioning and volume-binding infrastructure built for CronJobs/ephemeral volumes (see "Future Work" below)

**Key constraints:**
- Absolute requirement: large ephemeral scratch storage per job (up to 250GB for osmdiffs; other jobs in the family may need less)
- Multiple distinct cronjobs, potentially running concurrently — resource sizing, naming, and cost tracking must be per-job, not hardcoded to one workload
- Cheap indie developer infrastructure (total ~€50-60/month across the whole cronjob family)
- Must survive control plane restarts (crash recovery via reconciliation loop) — including a restart caused by updating the shim's own container image
- Must prevent resource leaks (volumes, VMs) if any component fails
- Must allow multiple independent, concurrently running kube-shim instances against the same cloud account without naming collisions or cross-instance orphan-scan interference — enforced via a configurable `resource_prefix` (default `"kube-shim"`), not a hardcoded literal
- Must prevent runaway cost if a single job runs long or several run at once — enforced by a rolling (not daily-reset) budget guard alone, so the daily rate can be low while still allowing an occasional bigger job funded by unspent prior days; no separate job-count cap, since at the budget levels this project runs at, the budget itself already prevents meaningful concurrency in practice
- The authenticated API must not be usable by anyone who doesn't hold a valid bearer token, since it can create jobs that spend the cloud budget — and this has to work without a VPN or other special networking
- kube-shim's own TLS certificate must be real and CA-trusted for a real hostname, issued and renewed with no human running `certbot` or pasting certs around — but a cold-start ACME failure (no cert yet, issuance broken) must fail loudly rather than silently serving broken or self-signed TLS to real clients, while a renewal failure for an already-valid cert must retry quietly in the background instead of taking the service down
- Worker VMs must be unreachable from the internet (no inbound connections from anyone but the shim itself), while still able to reach the internet outbound (downloading input data, uploading results)
- The public status page has no authentication, so it must never render secrets, credentials, or anything else that isn't meant to be public

---

## Architecture Overview

### Components

**1. Control Plane (Shim):** Rust binary running on a cheap VPS, as a rootless podman container
   - Axum HTTP server (port 443, TLS via ACME): the authenticated Kubernetes API, wrapped in bearer-token auth middleware, plus a small, separately-scoped public sub-router (status page + `/healthz`/`/livez`/`/readyz`) deliberately kept outside that middleware's scope — same listener, same certificate, two different route groups, not two different ports
   - A minimal, unauthenticated Axum router (port 80, plain HTTP) that serves *only* `/.well-known/acme-challenge/*` for Let's Encrypt HTTP-01 validation — nothing else is ever registered on this listener
   - SQLite for persistent state (jobs, volumes, VMs, events, pricing, budget balance), stored on a bind-mounted host directory so it outlives container restarts/updates
   - Reconciliation loop (10-second fallback tick, plus immediate wake-ups on specific events) for state management
   - Cloud provider client (UpCloud first) behind a common interface, + pricing sync

**2. Worker VMs:** Short-lived UpCloud Cloud Server instances (created per job run)
   - Receive volume attachment + container spec via cloud-init
   - Run Podman container with mounted volume
   - Communicate status/logs back to shim via SSH (a native Rust client, not a subprocess) + status files
   - Protected by an UpCloud firewall: no inbound from anywhere except the shim's own VPS IP, all outbound allowed — unreachable from the internet without needing private networking or a NAT gateway

**3. Kubernetes API Surface:** Minimal implementation
   - Resources: `Secret` (v1), `CronJob` (batch/v1) — with an inline generic ephemeral volume in the pod template for scratch storage — Pod status
   - Operations: CRUD on resources, watch/streaming, list
   - Extensions: Events, Metrics, Cost tracking + CSV cost report (custom APIs)
   - **Future:** `Deployment` (apps/v1) for long-running workloads, and standalone `PersistentVolumeClaim`/`PersistentVolume` for data that must survive across job runs — both deliberately deferred, but the internal `WorkloadKind` abstraction introduced in Phase 5 is designed so adding `Deployment` later doesn't require reworking the CronJob path.

**4. Public Status Page + Health Endpoints:** static-ish HTML plus a few standard diagnostic paths, served from `:443` alongside the authenticated API (same listener/certificate, a structurally separate GET-only sub-router kept outside the auth middleware)
   - `/` and a `/statusz` alias (the alias name nods to the informal "zPages" debug-page tradition from gRPC/OpenCensus, not a literal Kubernetes API-server convention the way the health endpoints below are): recent events, currently running jobs ("nodes"/pods), accumulated cost and budget balance in `main_currency`
   - `/healthz`, `/livez`, `/readyz`: genuinely standard, Kubernetes-API-server-defined health endpoints, unauthenticated by the same convention real clusters use for infra health checks
   - `/metrics` (Prometheus self-instrumentation) is a placeholder for later, not built in this round — see "Future Work" — distinct from the already-planned, authenticated `metrics.k8s.io` API (Phase 12), which reports job resource usage, not the shim process's own metrics
   - Read-only, no auth, no `kubectl`/VPN required for any of the above

### Data Flow

```
Terraform (terraform apply, with a bearer token configured)
  → HTTPS POST to Shim:443 (kubernetes_provider), TLS terminated with the
    shim's ACME-issued certificate, auth middleware checks the bearer
    token before anything else runs
    → Shim stores Secret / CronJob (incl. its inline ephemeral-volume spec)
      in SQLite
    → Reconciliation loop detects new jobs (woken immediately via
      tokio::sync::Notify, not waiting for the next 10s tick)
      → Checks the rolling budget balance (in main_currency); if the
        job's estimated cost exceeds it, the job waits in BudgetWait
        until enough has accrued
      → Launches a VM via the cloud provider interface, with cloud-init,
        sized from the pod template's resource requests, requesting an
        ephemeral volume at the size/tier the pod template specifies
      → Cloud-init mounts volume, pulls image, podman run
      → Container writes to /scratch, streams logs
      → On exit: Shim detects completion, fetches logs, deletes VM and
        its ephemeral volume (always — nothing is ever retained),
        decrements the budget balance by the job's actual cost
    → Shim updates job status in SQLite
    → Terraform reads back status via GET requests
  → kubectl logs -f osmdiffs-weekly-...  (streams from VM via SSH)
  → kubectl get pod, kubectl describe, etc. (reads from Shim database)
  → kubectl get cronjobs  (lists the whole job family, not just osmdiffs)
  → Browser: GET https://kube-shim.brawer.ch/  (public status page —
    events, running jobs, accumulated cost; read-only, no auth, same
    listener/cert as the authenticated API, just a different route group)
  → GET https://kube-shim.brawer.ch/healthz  (unauthenticated health
    check, standard Kubernetes convention)
  → GET https://kube-shim.brawer.ch/apis/cost.kube-shim.io/v1/report
    (authenticated CSV cost report, grouped by job labels)

Separately: release-please merges its release PR, tags vX.Y.Z
  → GitHub Actions builds a static musl binary, publishes a multi-arch
    ghcr.io/brawer/kube-shim:vX.Y.Z and :latest (FROM scratch, public,
    no pull auth), with SLSA Build Level 3 provenance
  → The kube-shim.brawer.ch instance's podman-auto-update.timer picks up
    :latest and restarts the container on its own — no SSH session needed
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
- Missing or wrong token → HTTP 401 Unauthorized, using the standard Kubernetes `Status` object (`reason: Unauthorized`) — distinct from the 403 `Forbidden` used for admission denials (Phase 5): 401 means "I don't know who you are," 403 means "I know who you are and the answer is no," matching real API server semantics.
- The public status page and health endpoints (Phase 14) are deliberately *not* wrapped by this middleware — even once they end up sharing the same `:443` listener as this API (Phase 4/14), the middleware stays scoped to just the authenticated API's own sub-router rather than blanket-applied to the whole listener, so the public routes stay unauthenticated by design.
- No custom client tooling needed: `kubectl config set-credentials ... --token=...` and Terraform's `kubernetes_provider` `token` argument both send exactly this header natively, regardless of how many tokens the shim currently considers valid.
- **From this phase onward, every `curl` example against the authenticated API elsewhere in this document assumes `-H "Authorization: Bearer $TOKEN"` is included — omitted from later snippets for brevity, not because it's optional.**

**Files to create/modify:**
- `src/auth.rs` (new) - bearer-token middleware: list iteration, constant-time comparison per entry, expiration check
- `src/k8s_status.rs` (new) - builds Kubernetes-shaped `Status` error responses (401 here; reused by Phase 5's 403 admission check and any future validation rejection)
- `src/main.rs` - wrap the authenticated router in the auth middleware, scoped so it doesn't also apply to the public status/health sub-router mounted later on the same listener (Phase 14)
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

# The public status page (once Phase 14 exists) must keep working with no auth at all:
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
**Goal:** A repeatable way to build, ship, and update the shim binary on the VPS: a `FROM scratch` OCI image built in CI, run rootless under podman — no VPN, no privileged ports.

**Deliverables:**
- Rust binary built statically for `x86_64-unknown-linux-musl` (and `aarch64-unknown-linux-musl` — see multi-arch below) — no libc dependency at all, required for `FROM scratch` to even start the binary. `sqlx`'s SQLite `bundled` feature and `rustls` (already the plan's TLS choice over OpenSSL) are both static-friendly, so this doesn't force a dependency change.
- TLS root certificates embedded in the binary via `webpki-roots`, not `rustls-native-certs` — a scratch image has no `/etc/ssl/certs`, so outbound HTTPS to the cloud provider's API needs its trust store compiled in.
- No shelling out to external binaries anywhere in the shim process: scratch has no shell and no `/usr/bin/anything` to exec. Phase 10's log streaming uses a pure-Rust SSH client library (`russh`) instead of invoking a system `ssh` binary. The same principle applies to any future subprocess-shaped need — reach for a Rust crate, not a bundled Linux distro.
- Runs as a non-root UID (1000) inside the container itself, on top of (not instead of) rootless podman on the host — defense in depth, and free, since the binary needs no special capabilities.
- **Multi-arch**: built for both `linux/amd64` and `linux/arm64` (via a GitHub Actions matrix, native builds on each architecture's own runner — no cross-compilation, `rust:1-alpine`'s default target already matches whichever arch it's running on), joined into a single multi-arch manifest. Revisits the original "amd64 only" plan: kept open for a possible future ARM VPS and consistency with this project's other repos, even though nothing currently requires it.
- GitHub Actions workflow: on a version tag push (now driven by `release-please`, not a manual `git tag` — see the release-please addendum below), builds and publishes `ghcr.io/brawer/kube-shim:vX.Y.Z` **and** `ghcr.io/brawer/kube-shim:latest`. The package is public, so the VPS needs no pull credentials at all.
  - **Revised from the original "never `:latest`" decision**: matching the convention used across this project's other repos (`:latest` alongside the immutable version tag, for convenience — e.g. quick manual testing) outweighed keeping kube-shim as a one-off exception. `:latest` turned out to be more than just a convenience tag: `deploy/kube-shim.container` tracks it directly via `AutoUpdate=registry`, so the real VPS deployment rolls forward on every release — see the auto-update note below.
  - **SLSA Build Level 3 provenance** is generated for every architecture-specific image and the joined manifest, via `actions/attest` (GitHub's native attestation action) run from a separate reusable workflow (`release-build.yml`) — not `slsa-framework/slsa-github-generator`, which an earlier version of this plan used; switched to match the same pattern already proven out in this project's sibling repos. The separate-workflow isolation (not a step folded into the same job that built the image) is what actually earns Level 3 rather than Level 1/2 either way — see `release-build.yml`'s own header comment.
  - A `verify-version` job cross-checks the pushed tag against `Cargo.toml`'s version before building anything, as a cheap sanity net (normally redundant, since `release-please` keeps them in sync itself, but cheap insurance against a stray manual tag).
- `bootstrap/provision.sh` rewritten around this: installs rootless podman; drops the earlier `scp` binary + hand-written systemd unit flow.
- A podman quadlet unit (`deploy/kube-shim.container`) defining: the image reference, bind mounts for persistent state (`/var/lib/kube-shim` on the host → `/data` in the container — holds `db.sqlite`, the TLS certs, and `config.toml`), and port publishing. Ports were originally chosen as `:6443`/`:8080` specifically because both are >1024, letting rootless podman bind them with no special capability or sysctl tweak — **revisited in Phase 4**, where ACME requires standard `:443`/`:80` for the authenticated API and challenge responder respectively; **`:8080` is dropped entirely once Phase 14 lands**, once it turns out the public status page/health endpoints don't need a port of their own either — see Phase 14.
- Because job/volume/VM/budget state lives entirely in SQLite (not in-process memory) and worker VMs run independently of the shim process, restarting the container for an update is safe even with jobs in flight — the reconciliation loop just resumes on its next tick, using the schema migrations that already run automatically on startup (Phase 1).
- **Updating: two supported modes, not one.** The quadlet unit as checked in tracks `:latest` with `AutoUpdate=registry`, and `podman-auto-update.timer` (a systemd `--user` timer, enabled once during provisioning) polls the registry and restarts the container whenever a new image lands — so the actual `kube-shim.brawer.ch` instance rolls forward automatically on every release, with no SSH session required. This is a deliberate trade for development-loop speed while the project is young, not a general recommendation: an unattended rollout of an untested release is a real risk given the shim spends real money orchestrating cloud resources. It's judged acceptable here specifically because UpCloud's billing is prepaid with no auto-recharge — a bad release that misbehaves can burn at most the current prepaid balance, not an unbounded card charge, which bounds the downside of "wrong code ran unattended" to something already priced in. A deployment that doesn't have that backstop (a different provider with postpaid/card-on-file billing, or once this moves past personal-project status) should instead pin an explicit `vX.Y.Z` tag, drop the `AutoUpdate=registry` label, and go back to the deliberate `podman pull` + `systemctl --user restart kube-shim` flow — both modes are just a one-line edit to the same quadlet unit, not different infrastructure.

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
# Confirm db.sqlite state (jobs, volumes, budget balance) survived the restart
# Confirm an in-flight job's worker VM was untouched by the shim restart
```

---

### Phase 4: Automatic TLS via ACME (Days 4-5) — ✅ Complete
**Goal:** Replace Phase 1's self-signed bootstrap certificate with a real, automatically issued and renewed TLS certificate — Caddy-like: point a hostname at the shim and it manages its own valid certificate, no manual cert copying, no CDN required in front of it.

**Deliverables:**
- The authenticated Kubernetes API moves from `:6443` to standard `:443`, and a new minimal responder listens on `:80` for Let's Encrypt's HTTP-01 challenge — the two ports Caddy itself uses, and the two ports a plain hostname (`https://kube-shim.brawer.ch/`, no `:6443` in the URL) needs. This reverses Phase 3's original "both ports >1024, so rootless podman needs no special capability" choice.
  - **Verified hands-on** (not assumed) by standing up `kube-shim.brawer.ch` for real on UpCloud, ahead of this phase's actual code: rootless podman *can* publish a host port `<1024` with no special capability, once the host's `net.ipv4.ip_unprivileged_port_start` sysctl is lowered to `80` (`bootstrap/provision.sh` has a step for this via `/etc/sysctl.d/`) — that governs `rootlessport`, which does the actual low-port bind *in the host's own network namespace*. **First shipped this wrong**, set to `443` rather than `80`: the sysctl is a floor, not a specific-port allowlist, so `=443` still left `80` itself blocked, which broke only the ACME challenge responder specifically (the `:443` API kept working, making it easy to miss) — caught by actually attempting a real ACME issuance against the live box, not by code review. But that sysctl does **not** extend into the *container's* own network namespace either way: the shim process itself, running as non-root UID 1000 inside the container (Phase 3), still cannot bind `<1024` there, and fails at startup with a plain `Permission denied` if `config.toml`'s `server.port` is set to `443` directly. The fix isn't a capability grant (`--cap-add=CAP_NET_BIND_SERVICE`) — it's simpler: publish asymmetrically, host `443`/`80` → container-internal unprivileged ports (`PublishPort=443:8443` and `PublishPort=80:8080` in the quadlet unit, matching `server.port`/`acme_challenge_port` in `config.toml`). The binary never needs to bind a privileged port at all; only `rootlessport`, on the host side, does.
  - **A second real bug, also only found by attempting real issuance**: the first implementation had `main.rs` await `acme::setup()`'s entire cold-start wait — including Let's Encrypt actually validating the HTTP-01 challenge — *before* ever starting the `:80` listener that validation depends on. A straightforward deadlock: every attempt failed with "Connection reset by peer" (`rootlessport` accepted the connection host-side, nothing was listening container-side yet). Fixed by having `acme::setup()` start the challenge responder itself, as a background task, *before* entering the cold-start wait — `main.rs` no longer touches the challenge router at all, `setup()` just returns the acceptor.
- ACME client logic (issuance + automatic renewal) built on `rustls-acme` or equivalent, integrated with the existing `axum-server`/`rustls` TLS setup from Phase 1/3 rather than replacing it — `rustls-acme` in particular is designed to plug into exactly that stack (hands the TLS acceptor a certificate resolver that swaps in fresh certs on renewal, no listener restart needed).
- `:80` serves *only* `/.well-known/acme-challenge/{token}` — the same "explicit allowlist, not just an auth gate" pattern later used for the public status page/health endpoints on `:443` (Phase 14): every other path 404s, by construction, not by convention.
- Config: `hostname: Option<String>` (the real public DNS name to request a certificate for) and `acme_directory: String` — `"staging"` or `"production"` select Let's Encrypt's own two environments (staging has much higher rate limits but issues a certificate chain that isn't publicly trusted); any other value is used verbatim as a custom ACME directory URL (e.g. a local Pebble test server), rather than a closed enum, since that's one less thing to extend later for a case this cheap to support generically. Plus `acme_contact_email: Option<String>` (optional, Let's Encrypt only uses it for expiry/problem notifications), `acme_challenge_port: u16` (default `8080`), and `acme_cache_dir: String` (default `"acme-cache"`, but any real deployment sets it to `/data/acme-cache` — see below). All four are `#[serde(default)]`, so the config schema change is purely additive: the already-deployed `kube-shim.brawer.ch` `config.toml`, which predates all of this, keeps parsing and keeps behaving exactly as before (self-signed fallback) with zero edits required. `config-dev.toml` is left with no `hostname` at all (self-signed, as always); the real deployment's `config.toml` gets `hostname`/`acme_directory` uncommented deliberately, later, once DNS is confirmed ready — not automatically by `provision.sh`, since a cold-start ACME failure refusing to start would otherwise trigger on a box whose DNS isn't live yet.
- **Cold start vs. renewal failure handled differently, on purpose:** if the shim has no cached certificate yet (first boot, or a wiped `/data`) and ACME issuance fails, it retries (bounded by a 5-minute wall-clock `tokio::time::timeout`, not a fixed attempt count — `rustls-acme`'s own state machine paces retries with its own internal backoff, so the timeout is a ceiling on top of that rather than a reimplementation of it) and then **fails to start** rather than serving broken or absent TLS — an operator needs to know immediately that DNS/networking/rate-limits are misconfigured, not discover it when a client's TLS handshake mysteriously fails. A **renewal** failure for a certificate that's still valid, by contrast, is handed off to a background `tokio::spawn` task that only ever logs — the certificate already deployed keeps serving traffic regardless, since the acceptor's resolver only updates on a successful renewal event. A transient Let's Encrypt outage or rate-limit bump is a log line, not a self-inflicted outage.
- **Local dev/CI fallback:** when `hostname` is unset, ACME is skipped entirely and the shim falls back to the self-signed certificate at `tls_cert_path`/`tls_key_path` — exactly Phase 1's original (and until now, only) behavior, unchanged in `tls.rs`, now just demoted to "the fallback" rather than "the only option," selected by a `match &cfg.server.hostname` in `main.rs`.
- `deploy/kube-shim.container` updated: `PublishPort=443:8443` and `PublishPort=80:8080` (asymmetric host:container mappings — see the verified finding above; the binary itself keeps listening on unprivileged ports internally, `8443`/`8080`, matching `config.toml`'s `server.port`/`acme_challenge_port`) replace the old `:6443`/`:8080` mappings.
- `bootstrap/provision.sh` gains a step writing `net.ipv4.ip_unprivileged_port_start=80` to `/etc/sysctl.d/`, applied once during provisioning — required for `rootlessport` to publish `:443`/`:80` on the host side at all (see the verified finding above). Also picked up two more real, hands-on-verified fixes along the way, both worth generalizing beyond just this phase: installing `systemd-container` (`machinectl shell`, used by the script's own "Next steps," isn't installed by default on stock Ubuntu 24.04), and a `podman unshare chown 1000:1000` step on the data directory after every file in it exists — the container's non-root UID 1000 (Phase 3) does not map back to the host `kube-shim` user's own UID under rootless podman's user namespace, so a plain `chown kube-shim:kube-shim` alone leaves the bind mount unwritable from inside the container (fails at startup with "unable to open database file").
- **Revised from the original plan wording**: self-signed certificate generation stays in `provision.sh` unconditionally (not dropped from the "real deployment" path as originally sketched) — it's a harmless, always-available safety net regardless of whether ACME ends up configured, and removing it would have added script complexity for no real benefit. `hostname` is simply left commented out in the generated `config.toml`, with the reasoning (and how to enable it) written directly into that file's own comments.

**Files created/modified:**
- `src/acme.rs` (new) - ACME issuance/renewal on top of `rustls-acme`: `setup()` blocks (bounded by the cold-start timeout) until a certificate is deployed, then hands ongoing renewal to a background task
- `src/main.rs` - branches on `cfg.server.hostname`: `Some` drives `acme::setup()` and runs both the `:443`-published API listener (via the ACME acceptor) and the `:80`-published challenge listener concurrently (`tokio::try_join!`); `None` keeps the original single self-signed listener from Phase 1/3, unchanged
- `src/config.rs` - `ServerConfig` gains `hostname`, `acme_directory`, `acme_contact_email`, `acme_challenge_port`, `acme_cache_dir`, all `#[serde(default)]`
- `src/app.rs` - `server_header_layer()` factored out (shared by the authenticated API router and the new ACME challenge router) — see the `Server:` header addendum below
- `Cargo.toml` - `rustls-acme` (default features disabled, `ring` re-enabled explicitly in their place, matching `tls.rs`'s own crypto-provider choice for musl-friendliness), `tokio-stream`, `tower-http`'s `set-header` feature
- `deploy/kube-shim.container` - port publishing updated to the asymmetric `443:8443`/`80:8080` mappings
- `bootstrap/provision.sh` - the `ip_unprivileged_port_start` sysctl step, the `systemd-container` package, the `podman unshare chown` fix, and the new (commented-out-by-default) `config.toml` fields

**Addendum: `Server:` version header.** Not part of the original Phase 4 scope, but small enough to fold into the same change: every response, from every router (the authenticated API and the ACME challenge router alike), now carries `Server: kube-shim/x.y.z`, stamped from `Cargo.toml`'s own package version at compile time (`env!("CARGO_PKG_VERSION")`) via a shared `tower_http::set_header::SetResponseHeaderLayer` applied as the outermost layer — including on a `401`, since knowing which release actually answered a request is exactly what you want while debugging a stale deployment, auth failures very much included. Verifiable with nothing more than `curl -I https://kube-shim.brawer.ch/`.

**Testing:**
```bash
# Local dev, no hostname configured: falls back to self-signed, exactly
# as Phase 1 already behaved (config-dev.toml's own configured port,
# unchanged -- ACME doesn't force local dev onto :443)
cargo run -- -c config-dev.toml
curl -k https://localhost:6443/api/v1
curl -sk -D - https://localhost:6443/api/v1 -o /dev/null | grep -i ^server:
#   -> server: kube-shim/x.y.z

# Real hostname, staging directory (safe to repeat without hitting
# production rate limits):
# config.toml: hostname = "kube-shim.brawer.ch", acme_directory = "staging"
# Confirm a certificate is issued, chain is the (untrusted) LE staging chain
curl --cacert <(curl -s https://letsencrypt.org/certs/staging/letsencrypt-stg-root-x1.pem) \
  https://kube-shim.brawer.ch/api/v1

# Cold-start failure: point hostname at a non-existent/unreachable DNS
# name and confirm the shim logs the failure and exits non-zero within
# ~5 minutes, rather than starting with no/broken TLS

# Renewal-failure resilience: force a renewal attempt to fail (e.g. block
# outbound to the ACME directory briefly) while an existing valid cert is
# in place; confirm the shim keeps serving the existing cert and retries
# in the background instead of restarting or refusing connections
```

---

### Phase 5: Generic Ephemeral Volumes + Multi-Job Resource Model (Days 5-6) — ✅ Complete
**Goal:** Let Terraform provision per-job scratch storage the same way real Kubernetes does for genuinely ephemeral storage — inline in the pod template — and remove the hardcoded single-workload assumptions from Phase 1 before the reconciliation loop is built on top of them.

**Deliverables:**
- Support for the CronJob pod template's `volumes: - name: scratch ephemeral: volumeClaimTemplate: spec: {resources.requests.storage, storageClassName}` field — real Kubernetes' "generic ephemeral volumes" (GA since 1.23) — instead of a standalone `PersistentVolumeClaim` resource type with its own CRUD handlers and a separate binding step. Simpler than this plan's original PVC-based design (see GitHub issue #16) and a better match for reality: nothing built here needs to survive across job runs, so there's no `Pending`/`Bound`/`Released` lifecycle, no cross-resource lookup, and no orphaned-claim bookkeeping to get wrong. Standalone `PersistentVolumeClaim`/`PersistentVolume` support for a job that *does* want to cache data across runs is deferred — see "Future Work" below.
- `spec.storageClassName` is repurposed from the original plan: since nothing is ever retained, it no longer selects a reclaim policy. Instead it selects a **performance tier** — two built-in, hardcoded class names, `kube-shim-standard` (the default when omitted) and `kube-shim-fast` — mapped internally, per cloud provider, to that provider's closest matching storage tier (UpCloud: `standard` / `maxiops`). No real `StorageClass` resource or general CSI-style pluggable parameters: providers in this space (UpCloud, and likely Infomaniak/Hetzner too) offer a small number of discrete storage tiers, not a continuously tunable IOPS number, and even real Kubernetes doesn't standardize IOPS as a portable parameter — that's left entirely to whichever CSI driver is bound. A small, fixed, hardcoded per-provider lookup table (`match classname { ... }` inside each `CloudProvider` implementation) is both simpler and more honest about what these providers actually offer than inventing a numeric abstraction they can't precisely honor. An unknown class name is rejected the standard way: HTTP 422, `Status` object, `reason: Invalid`, `causes: [{reason: FieldValueNotSupported, field: "...storageClassName"}]` — built via `src/k8s_status.rs` (Phase 2), same as every other structured error in this API.
- Database: `job_volumes` table (job_id, size_gb, storage_class_name, provider_volume_id, mount_point) — one row per job *run*, not a durable row reused across runs, since nothing here persists between runs. **Reserved schema only in this phase**: nothing writes to it yet, since there's no reconciliation loop (Phase 6) turning a CronJob's schedule into actual job runs for a row to represent.
- Per-job VM sizing: read `resources.requests.cpu` / `.memory` from the CronJob's pod template and map to a cloud-provider server size (small lookup table), instead of a fixed single size — needed now that the shim runs more than one workload shape. **Built as a standalone, tested `src/workload.rs` function** (`smallest_fitting_server_plan`), using real UpCloud plan names/specs confirmed against the actual API (Phase 7's own research) rather than placeholders — but deliberately a small, incomplete starting table: osmdiffs' real profile (6 CPU/8GB) already exceeds every plan listed, and finding its actual match is left to Phase 9 (VM provisioning), the same way every other provider-specific sizing decision in this plan gets settled hands-on rather than guessed in advance. Not wired into any live request path yet — nothing provisions VMs until Phase 9.
- Config: `resource_prefix` (default `"kube-shim"`), reserved here since this is the first phase that gives cloud resources shim-managed names — actually threaded through naming and orphan-scan matching in Phase 15. Also reserves the rolling-budget parameters (`budget_daily_rate`, `budget_rollover_cap_days`, in `main_currency` — see Phase 13) and `main_currency` itself — this just reserves the config shape; the accrual/enforcement/conversion logic is built in Phase 13, once cost calculation and ECB-rate sync exist. **Landed as a new top-level `[shim]` config section** (not nested under `[server]` or any provider section, since none of these four fields is specific to the HTTP listener or to UpCloud) — `#[serde(default)]` throughout, so the already-deployed `kube-shim.brawer.ch` config.toml (which predates this section entirely) keeps parsing unchanged.
- Internal `WorkloadKind` enum (`CronJob` for now), in `src/workload.rs`. Purely an internal abstraction — no new API surface, and not yet referenced by any reconciliation code (there isn't any until Phase 6) — done now so the future Deployment support (see "Future Work") doesn't require rewriting this layer once it exists.
- **Admission check on CronJob create/update: `spec.jobTemplate.spec.activeDeadlineSeconds` must be set.** `activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim needs a hard worst-case runtime bound for every job to make the budget guard (Phase 13) and deadline enforcement (Phase 11) meaningful, so it requires it via policy the same way a real cluster's `ValidatingAdmissionPolicy`/webhook would. A CronJob submitted without it is rejected with the same response shape a real admission webhook denial produces: HTTP 403, a `Status` object (`kind: Status`, `reason: Forbidden`, built via Phase 2's `src/k8s_status.rs`), message `admission webhook "kube-shim.io/require-active-deadline" denied the request: spec.jobTemplate.spec.activeDeadlineSeconds must be set (bounds the job's worst-case cost against the budget guard)`. `kubectl`/Terraform surface this exactly like any real admission denial — resolves Open Question 9. **Only wired into `create_cronjob`**: there's no update/PATCH handler yet for CronJobs (Phase 1 only ever built create/read/list/delete), so "on update" is aspirational until that exists.
- `src/k8s_status.rs` (Phase 2) gained a second builder, `invalid_field_value()`, alongside the existing `status_error()` — produces the full real-Kubernetes shape for a field-validation failure (`Status.details.causes[]`, `reason: FieldValueNotSupported`), not just the message string, so a programmatic consumer can find exactly which field was wrong.

**Files created/modified:**
- `src/api/cronjob.rs` - run both admission checks on `create_cronjob`, before anything is written; handler now returns `axum::response::Response` directly (unifying admission-rejection responses with the existing DB-error paths) rather than `Result<_, (StatusCode, String)>`
- `src/admission.rs` (new) - `require_active_deadline_seconds()` and `validate_ephemeral_volume_storage_classes()`, both reusing `src/k8s_status.rs` for the response. Return `Option<Response>` rather than `Result<(), Response>` — `Response` is too large for clippy's `result_large_err` lint to accept as an `Err` variant, and `Option` reads just as clearly at the call site (`if let Some(rejection) = ...`)
- `src/volumes.rs` (new) - `StorageTier` (`storageClassName` → tier → UpCloud tier name lookup) and `parse_storage_quantity_gb()` (Kubernetes resource-quantity parsing, binary/decimal suffixes, ready for `job_volumes.size_gb` once Phase 6+ needs it)
- `src/k8s_status.rs` - add `invalid_field_value()`
- `src/db/schema.sql` - add `job_volumes` table (reserved, unpopulated) + its index
- `src/config.rs` - new `ShimConfig`/`[shim]` section: `resource_prefix`, `main_currency`, `budget_daily_rate`, `budget_rollover_cap_days`
- `src/workload.rs` (new) - `WorkloadKind` enum + `ServerPlan`/`smallest_fitting_server_plan()`
- `tests/cronjob_admission_test.rs` (new) - both admission checks exercised through the real router (same construction `main.rs` uses), not just the underlying functions in isolation

**Testing:**
```bash
cargo test   # 79 tests: unit tests for StorageTier/parse_storage_quantity_gb/
             # smallest_fitting_server_plan/ShimConfig defaults, plus
             # tests/cronjob_admission_test.rs exercising both admission
             # checks through the real HTTP router

# Admission check (activeDeadlineSeconds):
curl -X POST .../cronjobs -d '{"spec": {"jobTemplate": {"spec": {"template": {...}}}}}'
# 403 Forbidden, Status object:
# admission webhook "kube-shim.io/require-active-deadline" denied the request: ...

# Storage tier:
curl -X POST .../cronjobs -d '{"spec": {"jobTemplate": {"spec": {
  "activeDeadlineSeconds": 3600,
  "template": {"spec": {"volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
    "storageClassName": "premium-ultra-disk"
  }}}}]}}
}}}}'
# 422 Unprocessable Entity, Status object with details.causes[0].reason ==
# "FieldValueNotSupported"; storageClassName: kube-shim-fast (or omitted,
# or kube-shim-standard) all accepted (201)
```

---

### Phase 6: Reconciliation Loop Skeleton (Days 6-7) — ✅ Complete
**Goal:** Build the state machine that will drive all orchestration.

**Deliverables:**
- Reconciliation loop: `tokio::time::interval` every 10 seconds as the **fallback** cadence — not the only trigger. A `tokio::sync::Notify` lets specific events wake the loop immediately instead of waiting up to 10s: a new `CronJob` submitted via the API, and (once Phase 13 exists) the budget balance crossing a job out of `BudgetWait`. The 10s poll stays in place regardless, since it's still what catches external state changes the shim wouldn't otherwise hear about (e.g. a provider-side VM failure) — the `Notify` is purely a responsiveness improvement for the cases the shim already knows about immediately, not a replacement for polling. `tokio::time::interval`'s *first* tick fires immediately by design, so the loop's very first pass also happens right at startup, not after the first 10s — a genuine feature (reconcile immediately on boot), not something worked around.
- **A piece the original plan text didn't spell out but turned out to be load-bearing: something has to turn a `CronJob`'s schedule into actual job runs** — otherwise the `jobs` table (which the rest of this phase reconciles) would only ever have zero rows. Built as `src/reconcile/schedule.rs`, the shim-internal equivalent of real Kubernetes' cronjob controller: parses `spec.schedule` (standard 5-field POSIX cron, via the `cron` crate — prepending a fixed `"0 "` seconds field to bridge to the 6-field form that crate expects) and creates a new `jobs` row whenever at least one scheduled instant has passed since the last run (or since the `CronJob`'s own creation, if it's never run) — coalescing any missed instants into a single catch-up run rather than bursting one per missed minute, the same way real `Allow`-policy CronJobs behave. There's no `concurrencyPolicy` support (`Forbid`/`Replace`) — nothing in this plan needs it.
- Job state machine: the full pipeline from "Reconciliation Loop State Machine" under Key Implementation Details (minus `BudgetWait`, which needs Phase 13's budget guard to mean anything) — one state per reconciliation tick, mocked (no real work happens, `Succeeded`/`Failed` collapses to always `Succeeded` since there's no real container execution yet to have an outcome).
- Database updates for job status, volume_id, worker_vm_id, etc.
- Error handling + retry tracking per job — deferred in substance to Phase 10 (nothing can actually *fail* yet, since every transition is mocked); this phase just makes sure `retry_count`/`version`/`last_transition_time` get touched correctly on every advance, so Phase 10 has real bookkeeping to build retry logic on top of rather than adding it from scratch.
- Startup reconciliation (detect orphaned jobs from crashed shim) — `src/reconcile/startup.rs` logs every non-terminal job found at boot and confirms it resumes reconciling normally on the very next tick. No actual cleanup logic yet (nothing external exists to clean up) — that's Phase 10.

**A real, discovered gap, not part of the original plan — found and fixed in this same phase:** `src/db/mod.rs`'s "migrations" only ever re-ran `CREATE TABLE IF NOT EXISTS`/`CREATE INDEX IF NOT EXISTS` — there was no `ALTER TABLE` step, so adding a column to an *already-existing* table would have silently never reached the already-deployed `kube-shim.brawer.ch` database (a `CREATE TABLE IF NOT EXISTS` against a table that already exists, missing the new column, is a no-op). This phase's schedule-due-ness check (below) doesn't actually need a new column — deriving "was this schedule already handled?" from `MAX(jobs.created_at) WHERE cronjob_name = ...` is arguably the better design on its own merits regardless (see that function's own comment for why) — but the underlying gap was real and worth closing properly rather than leaning on "we happened not to need one this time." Fixed with **`src/db/migrations.rs`**: a small, explicit `(table, column, type)` list, applied idempotently on every startup — checked via `PRAGMA table_info` first, so it's safe to run against both a database that predates the column and a fresh one where `schema.sql` already includes it. Empty for now (nothing currently needs it), but real and tested (`apply_added_columns()` is exercised directly against an explicit test list, not just the empty production one), so the mechanism is proven before the day it's actually needed. New columns on an existing table go here from now on, not by editing that table's `CREATE TABLE` in `schema.sql` after it's already shipped.

**Files created/modified:**
- `src/reconcile/mod.rs` (new) - reconciliation loop entry point (`run`/`tick`), `Notify`-based wake-up alongside the fallback interval
- `src/reconcile/job.rs` (new) - the state sequence + `next_state()`, `advance_all()`
- `src/reconcile/schedule.rs` (new) - turns a due `CronJob` schedule into a new `jobs` row (see above — not in the original file list, but necessary for this phase to do anything observable)
- `src/reconcile/startup.rs` (new) - startup recovery logic
- `src/api/cronjob.rs` - `create_cronjob` now actually stores `spec.schedule` (previously hardcoded to `""`, a stale placeholder from an earlier phase-numbering pass that never got filled in) and wakes the reconciliation loop via `Extension<Arc<Notify>>` after a successful insert
- `src/app.rs` - `build_router()` takes the shared `Notify` via `Extension`, not `State`, so only `create_cronjob` needs to know about it
- `src/main.rs` - runs startup recovery, then spawns the reconciliation loop as a background task before serving any requests
- `src/db/schema.sql` - **no changes**: `retry_count`, `last_error`, `volume_id`, `worker_vm_id` (and everything else this phase touches) were already present from Phase 1's original schema, which front-loaded far more of the eventual column set than that phase's own scope suggested at the time
- `src/db/migrations.rs` (new) - the `ALTER TABLE`-based column-migration mechanism described above
- `src/db/mod.rs` - runs `migrations::run()` right after the existing `schema.sql` pass

**No external API calls yet.** Just:
- Poll SQLite for jobs not in terminal state
- Mock state transitions (`Created` → `VolumePending` → `VolumeCreating` → ... → `Archived`)
- Log state changes

**Testing:**
```bash
cargo test   # 99 tests total, incl. unit tests for next_state()/schedule
             # due-ness/startup recovery/the ALTER TABLE migration
             # mechanism, plus a timing-based test proving Notify wakes
             # the loop faster than a (deliberately long) fallback interval

# Real end-to-end, not just unit tests: create a CronJob with an
# every-minute schedule against a live local server, then watch actual
# job rows appear and advance over real wall-clock time:
curl -k -X POST https://localhost:6443/apis/batch/v1/namespaces/default/cronjobs \
  -H "Authorization: Bearer $TOKEN" -d '{..."schedule": "* * * * *"...}'
sqlite3 db.sqlite "SELECT name, cronjob_name, status FROM jobs"
# Confirmed: one job row per elapsed minute, each independently advancing
# through the mocked pipeline (older ones reaching Archived, newer ones
# still partway through) -- verified hands-on, not just asserted in a test
```

---

### Phase 7: Cloud Provider Integration (Dry-Run Mode) (Days 7-8) — ✅ Complete
**Goal:** Call the real UpCloud API but don't actually create resources yet — and define the provider interface so UpCloud isn't hardcoded throughout the codebase.

**Deliverables:**
- A `CloudProvider` trait (`create_volume`, `delete_volume`, `attach_volume`, `detach_volume`, `create_server`, `get_server`, `delete_server`, `create_firewall_rules`, `list_firewall_rules`, `get_pricing`) defined *before* writing any UpCloud-specific code. Two additions beyond the plan's original list, both directly implied by the async-quirks bullet below rather than guessed: `get_server` (for Phase 9's create-then-poll loop) and `list_firewall_rules` (for Phase 9's apply-then-verify step) — a trait that only exposed `create_*` with no way to check on an async operation's real state wouldn't actually let the reconciliation loop model those quirks. `create_firewall_rules` is what Phase 9 uses to lock worker VMs down to no inbound traffic except from the shim itself. `UpCloudProvider` is the only implementation for now. This is what lets Hetzner and Infomaniak (OpenStack) be added later as additional implementations instead of a rewrite (see "Future Work") — but it's a lightweight seam, not a finished multi-cloud abstraction; expect its exact method signatures to need adjustment once a second provider is actually implemented against it.
- UpCloud client: no official Rust SDK exists, so this is a hand-rolled REST client on `reqwest` (already a dependency) rather than a provider-specific crate like the originally-planned `hcloud`. UpCloud's API (`https://api.upcloud.com/1.3/...`) uses `Authorization: Bearer <token>` the same shape as this project's own bearer-token auth (Phase 2), which simplifies the config story: `[upcloud] token = "..."` alongside the same kind of secret already handled elsewhere. Every endpoint shape used (`POST`/`DELETE /1.3/storage`, `POST /1.3/server/{uuid}/storage/attach`\`/detach`, `POST /1.3/server`, `DELETE /1.3/server/{uuid}`, `POST`/`GET /1.3/server/{uuid}/firewall_rule`, `GET /1.3/price`) was verified against UpCloud's own API reference directly (fetched and grepped for the real `METHOD /1.3/...` lines, not summarized/guessed) before being implemented — matching how every other UpCloud detail in this plan was already established.
- **UpCloud's API has real asynchrony/timing quirks the reconciliation loop needs to model explicitly, discovered from its own API reference rather than assumed:**
  - `POST /1.3/server` (create) is **asynchronous** — the response returns before the server is actually ready, so a `VMCreating` → `VMRunning` transition needs an explicit poll/wait step, not a synchronous "create returns a ready server" assumption.
  - `POST /1.3/storage` (create volume) is synchronous; attach/detach go through their own dedicated endpoints and are also synchronous, and can run against a live (already-running) server.
  - Firewall rules (`POST`/`PUT /1.3/server/{uuid}/firewall_rule`) are **asynchronous in effect**: the API call returns immediately, but the rule takes roughly 1-2 minutes to actually apply. This matters for a security-relevant guarantee ("worker VM is unreachable inbound") — the reconciliation loop must not consider a worker VM's network isolation established the instant the firewall API call returns; it needs its own explicit wait/verify step before the VM is treated as safe to leave running unattended (see Phase 9).
  - `GET /1.3/price` returns prices in the account's own billing currency — for this project's UpCloud account, that's EUR (UpCloud doesn't offer CHF billing), which is exactly why Phase 13's `main_currency` conversion exists.
- `dry_run` config flag (already existed on `UpCloudConfig` since Phase 1's original scaffold — this phase is the first to actually read it). New `[upcloud] zone` field alongside it (default `"de-fra1"`, the zone `kube-shim.brawer.ch` itself runs in) — real multi-zone/`nodeSelector` support stays deferred (see "Future Work"), so this is one shim-wide default, not per-job, for now.
- Reconciliation step: `VolumePending` → attempt volume creation (logged, not executed) — implemented in `src/reconcile/job.rs`, extracting the requested size/tier from the job's own stored pod-template spec (via `src/volumes.rs`'s `parse_storage_quantity_gb`/`StorageTier`, both built in Phase 5 and getting their first real caller here).
- Reconciliation step: `VMPending` → attempt VM creation (logged, not executed) — same pattern, extracting `resources.requests.cpu`/`.memory` and resolving them via `src/workload.rs`'s `smallest_fitting_server_plan` (Phase 5) plus a new `parse_cpu_cores` helper (CPU quantities use their own `"2"`/`"500m"` format, distinct from the `Ki`/`Mi`/`Gi` format memory shares with storage).
  - **On `dry_run`'s actual effect right now**: both log lines above fire regardless of `dry_run`'s value, since Phase 7's own explicit scope is "don't actually create resources yet" — there's no real call for `dry_run=false` to fall back to until Phase 8 exists. The flag is genuinely threaded through and read (`main.rs` → `reconcile::run` → `job::advance_all`), so Phase 8 only has to add the real branch, not build the plumbing to reach it.
- Error handling for UpCloud API authentication failures: `ProviderError::AuthenticationFailed`, returned distinctly from a generic `ProviderError::Api` for any `401`/`403` response. Consumed by a best-effort startup connectivity check (`GET /1.3/account`) in `main.rs`, logged as a warning on failure, **never fatal** — the already-deployed `kube-shim.brawer.ch` config still has `upcloud.token = "REPLACE_ME"`, so a hard failure here would crash-loop that instance on its next auto-update for no operational reason (nothing in this phase actually depends on UpCloud working yet).

**Files created/modified:**
- `src/providers/mod.rs` (new) - `CloudProvider` trait + request/response types (`CreateVolumeRequest`, `Volume`, `CreateServerRequest`, `Server`, `FirewallRule`, `PriceEntry`, `ProviderError`)
- `src/providers/upcloud/mod.rs` (new) - `UpCloudProvider`: the shared `reqwest` client/auth/error-mapping plumbing, plus `check_connectivity()`
- `src/providers/upcloud/volumes.rs` (new) - `create_volume`/`delete_volume`/`attach_volume`/`detach_volume`
- `src/providers/upcloud/servers.rs` (new) - `create_server`/`get_server`/`delete_server`, incl. the public-IP extraction real server responses need
- `src/providers/upcloud/firewall.rs` (new) - `create_firewall_rules` (one `POST` per rule -- UpCloud's own create endpoint has no bulk form) / `list_firewall_rules`
- `src/providers/upcloud/pricing.rs` (new) - `get_pricing`, extracting one `(zone, price_key)` entry from the full catalog
- `src/volumes.rs` - no changes needed; `parse_storage_quantity_gb`/`StorageTier` (Phase 5) got their first real caller
- `src/workload.rs` - add `parse_cpu_cores`
- `src/reconcile/job.rs` - add the dry-run-aware logging at `VolumePending`/`VMPending`
- `src/config.rs` - add `[upcloud] zone` (`dry_run` already existed)
- `src/main.rs` - construct `UpCloudProvider`, run the best-effort startup connectivity check, thread `dry_run` into `reconcile::run`

**Testing:**
```bash
cargo test   # 115 tests total. Every UpCloudProvider method is tested
             # against a real local HTTP server (axum, OS-assigned port --
             # the same pattern tests/tls_integration_test.rs already uses),
             # exercising the real request-building and response-parsing
             # round trip, not a hand-mocked reqwest layer.

# Real hands-on verification against the actual UpCloud API (not just
# mocks), using a throwaway example binary written for this, run once by
# hand, then discarded -- not part of the committed test suite:
#   check_connectivity()  -> OK
#   get_pricing("de-fra1", "server_plan_DEV-1xCPU-1GB-10GB")
#     -> amount=1 price=0.4464 (matches the exact value found during
#        Phase 7's original UpCloud API research)
#   create_volume(1GB, standard, "kube-shim-phase7-verify") -> real UUID
#   delete_volume(that UUID) -> confirmed gone via a follow-up
#     GET /1.3/storage/normal listing (no orphan left behind)

# Apply osmdiffs CronJob (small test version, 1GB volumes)
terraform apply
# Watch logs:
# "DRY-RUN: would create volume for job default/osmdiffs-weekly-...: 1GB, tier Standard"
# "DRY-RUN: would launch VM for job default/osmdiffs-weekly-...: plan DEV-...(...)"
# Verify NO resources created in UpCloud's control panel
```

---

### Phase 8: Real UpCloud Operations (Small Volumes) (Days 8-9)
**Goal:** Actually provision volumes, but small (1-10GB) for safety.

**Deliverables:**
- Set `dry_run = false` in config
- **The `UpCloudProvider` HTTP client itself (`create_volume`/`delete_volume`/`attach_volume`/`detach_volume`) already exists, real and tested against the live API — built in Phase 7, ahead of this phase's original schedule, since implementing `CloudProvider` for real was cheaper to do together with defining the trait than to split across two phases.** This phase's actual remaining work is wiring those already-real calls into the reconciliation loop's `VolumeCreating`/`VolumeAttaching`/`VolumeAttached` states (replacing the `dry_run`-gated logging from Phase 7 with real calls + real error handling on the `dry_run=false` path), and populating `job_volumes` (reserved since Phase 5, still empty) with the resulting `provider_volume_id` once a volume is actually created.
- Volume attachment to the worker VM
- Error handling + retry logic with timeouts (5 min for volume creation)
- Orphan detection: scan UpCloud for storage/servers with our `resource_prefix`, delete if not tracked in DB
- Database: track `provider_volume_id`/mount point on `job_volumes` (Phase 5's reserved table) — `jobs.volume_device`/`.mount_point` already exist too, front-loaded in Phase 1's original schema (see Phase 6's own note on this pattern)

**Files to modify:**
- `src/reconcile/job.rs` - implement volume state steps (VolumeCreating, VolumeAttaching, VolumeAttached), calling the already-real `CloudProvider` methods
- `src/reconcile/orphan_scan.rs` (new) - periodic orphan detection + cleanup
- `src/db/schema.sql` - **no changes expected** — verify against the current schema before assuming a column is missing; Phase 1 front-loaded far more of this than any individual phase's own scope suggested at the time (see Phase 6 and Phase 7's own notes on this same pattern)

**Testing:**
```bash
# Watch UpCloud's control panel live
# terraform apply (with test job, 1GB volume, short timeout)
# Verify volume appears: kube-shim-vol-test-...
# Verify volume mounted on VPS: ssh root@vps ls -la /mnt/scratch-*
# terraform destroy
# Verify volume deleted (should be fast -- storage delete is synchronous)
# Repeat 5 times without orphans
```

**Exit criteria:**
- 10 cycles of create→delete volume with zero orphans
- UpCloud prepaid balance still near-untouched

---

### Phase 9: VM Provisioning + Cloud-Init (Days 9-10)
**Goal:** Launch actual UpCloud Cloud Server instances with containers inside.

**Deliverables:**
- Cloud-init script generation (bash, templated with proper escaping — job-supplied values such as image name/args/env must never be interpolated into shell unescaped; pass them as a base64-encoded blob decoded inside the VM instead)
- Real VM creation, sized per-job using the resource-request lookup table from Phase 5 — and, per Phase 7's finding, treated as **asynchronous**: a distinct `VMCreating` (waiting for UpCloud to report the server actually running) state before `VMRunning`, not an assumption that the create call itself returns a ready server.
- **Every worker VM gets UpCloud firewall rules attached at creation time**: all inbound denied except SSH (22) from the shim's own VPS IP; all outbound allowed. Because rule application lags the API call by roughly 1-2 minutes (Phase 7), the VM passes through an explicit `FirewallApplying` → `FirewallVerified` step (polling the rule's actual status, not just assuming success) before the reconciliation loop treats "unreachable from the internet" as true and lets the job proceed to running the container. This is what makes "unreachable from the internet, but can still reach it" true (Key Constraints) — no private networking, no NAT gateway, no extra routing needed; the VM keeps a normal public IP for outbound connectivity, it's just unreachable inbound from anyone but the shim, and the shim never trusts that before it's actually verified.
- VM waits for volume attachment, mounts it
- VM pulls container image, runs podman
- Container write to /scratch
- Status file: `/tmp/job-status.txt` (written by cloud-init on completion)
- Container ID saved: `/tmp/container-id.txt`
- Database: track worker_vm_id, worker_vm_name, worker_ssh_ip

**Files to create/modify:**
- **`src/providers/upcloud/servers.rs` and `firewall.rs` already have real, tested `create_server`/`get_server`/`delete_server`/`create_firewall_rules`/`list_firewall_rules` methods — built in Phase 7, ahead of schedule, same reasoning as Phase 8's volume methods.** This phase's actual remaining work is the create→poll-until-running loop and the apply→list-and-verify loop *using* those methods, both living in `src/reconcile/job.rs`, not in the provider client itself.
- `src/cloud_init.rs` (new) - generate cloud-init script with proper escaping
- `src/reconcile/job.rs` - add VM state steps (VMCreating, VMRunning, FirewallApplying, FirewallVerified, etc.), each calling the already-real `CloudProvider` methods
- `bootstrap/cloud-init-template.sh` (new) - bash template for VM startup
- `src/db/schema.sql` - **no changes expected** — `jobs.worker_vm_id`/`.worker_ssh_ip`/`.worker_vm_name`/`.exit_code` already exist, front-loaded in Phase 1's original schema (see Phase 6/7/8's own notes on this same pattern); verify against the current schema before assuming otherwise

**Test scenario:**
```bash
# Use busybox test image: writes 100MB to /scratch, exits 0
# terraform apply (test job with busybox)
# Watch:
#   1. Volume created + mounted
#   2. VM launched (watch UpCloud control panel)
#   3. VM IP becomes available (wait ~1 min for boot)
#   4. Firewall rules applied and verified (watch for the FirewallVerified
#      transition specifically -- confirm the shim does NOT start the
#      container before this)
#   5. Container starts
#   6. Writes to /scratch
#   7. Container exits
# Verify logs fetchable: ssh root@{ip} podman logs {container-id}
# terraform destroy
# Verify VM + volume deleted

# Firewall:
# From a machine that is NOT the shim's VPS, confirm every port on the
# worker VM's public IP is unreachable (e.g. `nc -zv {worker-ip} 22` times out)
# -- test this both immediately after FirewallVerified and again a few
# minutes later, to catch a false-verified state
# From inside the worker VM (via the shim's own SSH access), confirm outbound
# still works, e.g. `curl -sI https://example.com` succeeds
```

---

### Phase 10: Log Streaming (Days 10-11)
**Goal:** Support `kubectl logs -f` for live monitoring.

**Deliverables:**
- HTTP endpoint: `GET /api/v1/namespaces/default/pods/{name}/log?follow=true`
- SSH to the worker VM using the `russh` client library (Phase 3) — never a subprocess — run `podman logs -f {container-id}` there, stream output back to the HTTP client
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

### Phase 11: Reconciliation Hardening (Days 11-12)
**Goal:** Make the reconciliation loop bulletproof for production.

**Deliverables:**
- Comprehensive error handling for all UpCloud API calls
- Idempotent state transitions (can retry without side effects) — before creating a volume or VM, check for an existing UpCloud resource carrying this job's name/label, so a crash between "API call succeeded" and "DB write committed" can't create a duplicate on restart
- Timeouts: volume creation (5 min), VM boot (5 min), firewall verification (5 min)
- **`activeDeadlineSeconds` enforcement (hard, not advisory):** since Phase 5's admission check guarantees every job has one set, the reconciliation loop tracks each running job's deadline and force-kills its VM (delete, not just stop the container) the moment it's exceeded — mirroring real Kubernetes Job behavior, where a Job that outlives `activeDeadlineSeconds` is terminated and marked `Failed` with reason `DeadlineExceeded`. This is what makes Phase 13's cost estimate an actual worst-case bound rather than a hopeful guess: nothing can silently run (and bill) past the deadline it declared at submission time.
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
# Wait for UpCloud to create volume (may take a moment)
kube-shim (restart)
# Should detect orphan volume, delete it after ~5 min
sleep 10 && curl -k https://localhost:443/debug/scan-orphans

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
# Should show Failed, reason DeadlineExceeded; VM should be gone in UpCloud's panel
# Repeat once while killing the shim at t=30s, to confirm startup recovery also
# catches an expired deadline it missed while down
```

---

### Phase 12: Events + Metrics APIs (Days 12-13)
**Goal:** Add observability for `kubectl describe` and monitoring across the whole cronjob family, not just one workload.

**Deliverables:**
- Events table in SQLite: reason, message, timestamp
- Emit events during reconciliation: "VolumeCreated", "VMStarting", "FirewallVerified", "ContainerRunning", "BudgetWait", etc.
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

### Phase 13: Pricing + Cost Tracking + Budget Guard + Cost Report (Days 13-15)
**Goal:** Track pricing, calculate job costs in the operator's own currency, enforce a *rolling* budget guard, and expose a per-project cost breakdown.

**Deliverables:**
- Daily sync of UpCloud's pricing API (`GET /1.3/price`, returned in the account's billing currency — EUR for this project) → SQLite cache table, via `CloudProvider::get_pricing()` (Phase 7).
- Daily sync of the ECB's reference exchange rates (`https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml`) → SQLite cache table. The feed is EUR-anchored (every rate is EUR→X), which lines up exactly with UpCloud's EUR-denominated pricing — no cross-currency chaining needed, just `amount_eur × rate[main_currency]`.
- `main_currency` (Phase 5's reserved config field, e.g. `"CHF"`): all cost figures the shim *reports* — budget balance, per-job cost, the cost report below — are expressed in this currency, converted from the provider's real EUR cost via the day's ECB rate. The rolling-budget config fields are renamed from the original `budget_daily_rate_eur`/`balance_eur` to currency-agnostic `budget_daily_rate`/`balance` (implicitly in `main_currency` units) — hardcoding "_eur" in the name stopped making sense once the number displayed to the operator isn't always EUR, even though the underlying UpCloud spend always is.
- Cost calculation: (VM hourly price × duration) + (volume price per GB-hour × GB × duration), computed in EUR from provider pricing, then converted to `main_currency` for storage/display.
- Estimated cost before a job starts — now required, not optional, since the budget guard depends on it: hourly VM price × the job's `activeDeadlineSeconds` (its worst-case runtime) + estimated volume cost for that same duration. This is a true worst-case bound, not a hopeful guess: Phase 5's admission check guarantees every job has `activeDeadlineSeconds` set, and Phase 11 actually force-kills the VM if it's exceeded, so nothing can run past the duration this estimate assumes.
- Actual cost calculated when job completes
- Cost aggregated per job and across the whole family (daily/weekly/monthly totals)
- **Rolling budget guard**, replacing a flat daily cap:
  - `budget_daily_rate` (e.g. CHF 2/day) accrues into a persisted balance continuously, capped at `budget_daily_rate × budget_rollover_cap_days` (default 7 days) — so several quiet days build up enough headroom for one bigger job, without the balance growing unbounded if jobs never run
  - Before launching a job, its estimated cost must be ≤ the current balance. If not, the job enters a new `BudgetWait` state and is retried — immediately once the balance changes (via Phase 6's `Notify` wake-up), and on the fallback tick otherwise — not failed, until enough balance has accrued. This resolves Open Question 7's "hard stop vs. soft alert" in favor of a hard stop.
  - The balance decrements by the job's *actual* cost once it completes (not the conservative estimate), so a job that finishes early returns its unused margin to the balance for the next one.
  - New `budget_state` table: a single row with `balance`, `last_accrual_at`.
- **CSV cost report**, `GET /apis/cost.kube-shim.io/v1/report?from=...&to=...`, grouped by a Kubernetes label on the job (e.g. `app.kubernetes.io/name`, or a custom `kube-shim.io/project` label) — answering "which project ate how much budget," not just an aggregate total. Column naming is loosely inspired by the [FOCUS](https://focus.finops.org/) (FinOps Open Cost and Usage Specification) convention — `ServiceCategory`, `ChargeCategory`, `BilledCost`, `BillingCurrency`, `ChargePeriodStart`/`End`, a project/label grouping column — without implementing FOCUS itself: FOCUS is designed for cloud-billing exports at AWS-CUR/Azure-Cost-Management scale, far more machinery than a one-provider, handful-of-jobs report needs. Borrowing its column vocabulary just means the CSV opens sensibly in a spreadsheet and means roughly what a FinOps-familiar reader expects, nothing more.
- Expose cost and budget balance via the metrics API, events, and the public status page (Phase 14)
- Hardcoded fallback pricing (and a fallback fixed exchange rate) if either sync fails

**Files to create/modify:**
- `src/providers/upcloud/pricing.rs` (new) - UpCloud pricing API fetch, implements `CloudProvider::get_pricing()`
- `src/currency.rs` (new) - ECB daily-rate fetch/parse (XML — via `quick-xml` or similar) + EUR-anchored conversion to `main_currency`
- `src/pricing/mod.rs` (new) - provider-agnostic cost calculation + budget accrual/enforcement, in `main_currency`
- `src/api/pricing.rs` (new) - cost + budget-balance endpoint
- `src/api/cost_report.rs` (new) - CSV cost report endpoint, grouped by label
- `src/reconcile/pricing.rs` (new) - background pricing + ECB-rate sync loop + budget accrual tick + `BudgetWait` check
- `src/db/schema.sql` - add `provider_pricing`, `exchange_rates` tables; `estimated_cost` column; `budget_state` table

**Testing:**
```bash
# Check daily pricing + exchange-rate sync
journalctl -u kube-shim -f | grep -E "pricing|exchange rate"
# Should see both updated once per day

# After job completes
sqlite3 db.sqlite "SELECT name, estimated_cost FROM jobs"
# Should show a plausible CHF figure (or whatever main_currency is set to)

# Via API
curl -k https://localhost:443/debug/status | jq '.total_cost, .budget_balance, .currency'

# Cost report
curl -k https://localhost:443/apis/cost.kube-shim.io/v1/report?from=2026-09-01&to=2026-09-30
# CSV, one row per (project label, day), opens cleanly in a spreadsheet

# Rolling budget test:
# Set budget_daily_rate=2, budget_rollover_cap_days=7 (in main_currency)
# Let 3 days pass with zero jobs running (or fast-forward budget_state.last_accrual_at for testing)
# Confirm balance ~= 6 (in main_currency)
# Apply a job estimated at 5 -- should launch (balance covers it)
# Apply a second job estimated at 5 in the same tick -- should enter BudgetWait
#   (only ~1 left), then launch once enough balance has accrued
```

---

### Phase 14: Public Status Page + Health Endpoints (Days 15-16)
**Goal:** A simple, always-reachable, read-only landing page plus standard Kubernetes-style health endpoints — no VPN or `kubectl` needed — all served from the same `:443` listener and ACME certificate as the authenticated API, not a separate plaintext port.

**Deliverables:**
- A small, separately-scoped Axum `Router` — GET routes only, by construction, not just by an auth check, so a bug elsewhere can't accidentally expose a write path here — merged into the main `:443` app at specific paths, and deliberately excluded from Phase 2's bearer-token middleware. This requires that middleware to be scoped to just the authenticated API's own sub-router by this point (a small refactor of how Phase 2's wrapping is structured, not new behavior) rather than blanket-applied at the top of the whole `:443` app the way it could be back when `:6443`/now `:443` had only API routes on it.
- `/` and a `/statusz` alias (nodding to the informal "zPages" debug-page tradition from gRPC/OpenCensus, not a literal Kubernetes API-server convention): single server-rendered HTML page, auto-refreshing (`<meta http-equiv="refresh">` or a few lines of polling JS), showing:
  - Recent events (Phase 12)
  - Currently running jobs ("nodes"/pods): name, job type, elapsed time, VM size
  - Accumulated cost and current budget balance / rollover cap, in `main_currency` (Phase 13)
- `/healthz`, `/livez`, `/readyz`: standard, genuinely Kubernetes-API-server-defined health-check endpoints, unauthenticated by the same convention real clusters use (infra health checks — load balancers, monitoring — can't always present a token). `/livez` reflects whether the process itself is up; `/readyz` additionally reflects whether the reconciliation loop and DB are actually functioning; `/healthz` mirrors `/readyz`, kept for compatibility with tooling that only knows the older combined name.
- `/metrics` (a Prometheus self-instrumentation endpoint) is explicitly *not* built in this phase — deferred as a placeholder, see "Future Work: Prometheus `/metrics` Self-Instrumentation". Not to be confused with the already-planned, authenticated `metrics.k8s.io` API (Phase 12), which reports job/pod resource usage for `kubectl top`, a different concern with a different audience.
- An explicit allowlist of what's rendered on `/`/`/statusz` — job names, timestamps, event reasons/messages, cost figures. Secret values, S3 credentials, SSH details, and worker VM IPs must never appear here, since these routes have no authentication at all.
- `deploy/kube-shim.container`'s `:8080` port publish, left in place since Phase 3, is removed — nothing built across this whole plan ends up needing a port of its own beyond `:443`/`:80` (Phase 4).

**Files to create/modify:**
- `src/status_page.rs` (new) - renders the HTML page from DB reads (events, jobs, pricing/budget); also implements `/healthz`/`/livez`/`/readyz`
- `src/main.rs` - merge the public sub-router into the `:443` app, outside the auth middleware's scope; rescope that middleware to the authenticated API's own sub-router if it wasn't already
- `deploy/kube-shim.container` - remove the now-unused `:8080` port publish

**Testing:**
```bash
curl -k https://kube-shim.brawer.ch/
curl -k https://kube-shim.brawer.ch/statusz   # same content as /
curl -k https://kube-shim.brawer.ch/healthz
curl -k https://kube-shim.brawer.ch/livez
curl -k https://kube-shim.brawer.ch/readyz
# All of the above: no Authorization header sent, still 200 OK

# From a phone browser: https://kube-shim.brawer.ch/
# Confirm no Secret values, tokens, or SSH details appear anywhere in the page source
# Confirm POST/PUT/DELETE to any of the above paths all 404 (no such
# routes exist on the public sub-router)

# Confirm the public sub-router did NOT accidentally widen the
# authenticated API's own exemption:
curl -k https://kube-shim.brawer.ch/api/v1
# Still 401 Unauthorized with no Authorization header
```

---

### Phase 15: Resource Naming + Cleanup (Days 16-17)
**Goal:** Identify resources created by this shim instance specifically, and enable manual cleanup.

**Deliverables:**
- Naming convention: `{resource_prefix}-{type}-{job-name}-{timestamp}-{random}`, using the `resource_prefix` config field reserved back in Phase 5 (default `"kube-shim"`) instead of a hardcoded literal — this is what lets multiple independent kube-shim instances (e.g. a personal one and a work one) run concurrently against the same cloud account without naming collisions or, more importantly, without one instance's orphan scan mistaking another instance's live resources for orphans and deleting them.
- Orphan scan (Phase 8) updated to match on `resource_prefix` from config, not a hardcoded string, everywhere it currently does so.
- Manual cleanup script: deletes orphaned resources matching this instance's `resource_prefix`, older than 7 days
- Orphan scan runs periodically (every 5 min in reconcile loop)
- Database tracks resource names for easy lookup

**Files to create/modify:**
- `src/naming.rs` (new) - resource name generation, reading `resource_prefix` from config
- `bootstrap/cleanup-orphans.sh` (new) - manual cleanup script, `resource_prefix`-aware
- `src/reconcile/orphan_scan.rs` - switch from a hardcoded prefix to `resource_prefix`

**Testing:**
```bash
# Verify all UpCloud resources follow the naming convention
curl -H "Authorization: Bearer $TOKEN" https://api.upcloud.com/1.3/storage | jq '.storages.storage[].title'
# All should start with the configured resource_prefix (default "kube-shim-")

# Run two shim instances with different resource_prefix values (e.g.
# "kube-shim-" and "kube-shim-work-") against the same UpCloud account;
# confirm each instance's orphan scan only ever touches its own resources

# Manual cleanup script
./cleanup-orphans.sh
# Should delete only this instance's volumes/VMs older than 7 days
```

---

### Phase 16: Testing + Hardening (Days 17-18)
**Goal:** Run real workloads from the cronjob family end-to-end, verify end-to-end, monitor for 1 week.

**Deliverables:**
- Swap 1GB test volume for the real per-job sizes (up to 250GB for osmdiffs)
- Swap busybox for real container images, across at least osmdiffs and one other cronjob from the family
- Run osmdiffs and at least one other cronjob concurrently at least once, to exercise the budget guard under real concurrent-job conditions
- One full osmdiffs job run (6+ hours)
- Monitor UpCloud's control panel, logs, costs, and the public status page
- Run 2-3 scheduled runs across the family (wait for real schedule triggers or trigger manually)

**Testing:**
```bash
# Deploy the real CronJobs (osmdiffs + at least one more from the family)
terraform apply

# Monitor:
journalctl -u kube-shim -f
curl -k https://localhost:443/debug/status (every hour)
curl -k https://<hostname>/   # public status page, from your phone
# UpCloud control panel (watch volume + VM count across all jobs)

# After jobs complete
kubectl get cronjobs
kubectl logs osmdiffs-weekly-XXXXX
kubectl describe pod osmdiffs-weekly-XXXXX
curl -k https://localhost:443/debug/status | jq '.total_cost, .budget_balance'
```

---

## Critical Files Summary

| File | Purpose | Status |
|------|---------|--------|
| `Cargo.toml` | Rust dependencies | Phases 1, 3 |
| `src/main.rs` | Server entry point, both listeners (`:443`, `:80`) | Phases 1-4, 14 |
| `src/auth.rs` | Bearer-token authentication middleware for the API | Create (Phase 2) |
| `src/k8s_status.rs` | Kubernetes-shaped `Status` error responses (401 here, reused for 403/422) | Create (Phase 2) |
| `.github/workflows/release.yml` | CI: build musl binary, publish OCI image to ghcr.io | Create (Phase 3) |
| `Containerfile` | Multi-stage build → `FROM scratch` image | Create (Phase 3) |
| `deploy/kube-shim.container` | Podman quadlet unit (image ref, bind mounts, ports) | Phases 3, 4, 14 |
| `src/tls.rs` | TLS config: self-signed bootstrap/fallback (Phase 1, unchanged since) | Phase 1 |
| `src/acme.rs` | ACME (Let's Encrypt) issuance/renewal + `:80` HTTP-01 challenge router | Create (Phase 4) |
| `src/api/*.rs` | Kubernetes API handlers | Phases 1, 5, 12 |
| `src/volumes.rs` | `storageClassName` → provider storage-tier lookup | Create (Phase 5) |
| `src/workload.rs` | `WorkloadKind` abstraction (CronJob now, Deployment later) | Create (Phase 5) |
| `src/admission.rs` | `activeDeadlineSeconds`-required policy check | Create (Phase 5) |
| `src/db/schema.sql` | SQLite schema | Phases 1, 5-6, 8-9, 11-13 |
| `src/reconcile/*.rs` | State machine loop, `Notify`-based wake-up, `activeDeadlineSeconds` enforcement | Create (Phases 6, 8-9, 11, 13) |
| `src/providers/*.rs` | `CloudProvider` trait + UpCloud implementation | Create (Phases 7-9, 13) |
| `src/providers/upcloud/firewall.rs` | Firewall rules for worker VMs (create + list, both real since Phase 7; poll-until-applied logic itself is Phase 9) | Create (Phase 7) |
| `src/currency.rs` | ECB daily exchange-rate sync + conversion to `main_currency` | Create (Phase 13) |
| `src/pricing/*.rs` | Cost tracking + rolling budget guard, in `main_currency` | Create (Phase 13) |
| `src/api/cost_report.rs` | CSV cost report, grouped by job label | Create (Phase 13) |
| `src/ssh.rs` | SSH client built on `russh` | Create (Phase 10) |
| `src/status_page.rs` | Public status page + `/healthz`/`/livez`/`/readyz`, served on `:443` | Create (Phase 14) |
| `src/naming.rs` | `resource_prefix`-aware resource name generation | Create (Phase 15) |
| `src/config.rs` | Configuration parsing | Phases 1-2, 4-5, 7 |
| `config.toml` | Runtime config template | Create (Phase 1) |
| `bootstrap/provision.sh` | One-time VPS setup | Phases 1-4 |
| `bootstrap/cloud-init-template.sh` | VM startup script | Create (Phase 9) |
| `bootstrap/cleanup-orphans.sh` | Manual cleanup script | Create (Phase 15) |

---

## Key Implementation Details

### Dependencies (Cargo.toml)
```toml
axum = "0.7"
axum-server = { version = "0.6", features = ["tls-rustls"] }
tokio = { version = "1", features = ["full"] }
sqlx = { version = "0.8", features = ["sqlite", "runtime-tokio"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
k8s-openapi = "0.22"
tracing = "0.1"
tracing-subscriber = "0.3"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1"
config = "0.14"
reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
webpki-roots = "0.26"
russh = "0.45"
rustls-acme = "0.15"      # Phase 4 (shipped): ACME issuance/renewal against axum-server's rustls stack -- default-features disabled, "ring" re-enabled explicitly to match tls.rs's own provider choice
tokio-stream = "0.1"      # Phase 4 (shipped): drives rustls-acme's event stream
cron = "0.17"             # Phase 6 (shipped): parses CronJob schedules to trigger job runs
quick-xml = "0.36"        # Phase 13: parsing the ECB's daily exchange-rate feed
```
No dedicated cloud-provider crate: UpCloud has no official Rust SDK, so `src/providers/upcloud/` is a hand-rolled REST client on `reqwest` (Phase 7) rather than an `hcloud`-style dependency.

### Reconciliation Loop State Machine
```
Created → BudgetWait (if needed) → VolumePending → VolumeCreating → VolumeCreated
  → VolumeAttaching → VolumeAttached → VMPending → VMCreating → VMRunning
  → FirewallApplying → FirewallVerified → ContainerRunning → Succeeded/Failed
  → VolumeDetaching → VolumeDeleted → Archived
```

Each state has:
- Entry conditions (can we transition?)
- Action (what to do?)
- Exit condition (when to move to next state?)
- Timeout (how long before failing?)
- Retry logic (backoff on failure)

A job passes through `BudgetWait` before `VolumePending` if its estimated cost exceeds the current rolling budget balance (Phase 13); it's woken immediately when the balance changes (Phase 6's `Notify`), or retried on the fallback tick otherwise, rather than failed.

`VMCreating → VMRunning` and `FirewallApplying → FirewallVerified` are both explicit poll-until-true steps, not assumed-synchronous transitions — UpCloud's server-create and firewall-rule-apply calls both return before the underlying state is actually true (Phase 7). The reconciliation loop never lets a container start before `FirewallVerified`, since that's the transition that actually makes "unreachable from the internet" true.

A running job that exceeds its `activeDeadlineSeconds` (Phase 11) is force-killed and transitions to `Failed` with reason `DeadlineExceeded` — the same terminology real Kubernetes Jobs use for this exact situation.

### Authentication
The authenticated API (`:443`, Phase 4) requires `Authorization: Bearer <token>` on every request (Phase 2), checked against every non-expired entry in `api_tokens` with a constant-time comparison. A missing or wrong token gets a standard Kubernetes `Status` object with `reason: Unauthorized` (HTTP 401) — separate from the `reason: Forbidden` (HTTP 403) used for admission denials below, matching real API server conventions. `:80` (ACME challenges, Phase 4) and the public status/health sub-router mounted on `:443` (Phase 14) are both deliberately excluded from this middleware; they're meant to be public — the latter via scoping the middleware to just the authenticated API's own sub-router, not a separate listener. Supporting a *list* of tokens (each with an optional expiry) rather than a single one is what makes rotation possible: add a new token, migrate clients, then remove the old one — never a single atomic cutover with no overlap window.

Secrets (this project's own `api_tokens`, cloud-provider credentials, and K8s `Secret` object contents) are stored in SQLite/`config.toml` **in plaintext, not encrypted at rest** — a deliberate decision, not an oversight. The only realistic threat model where DB-file encryption would help is an attacker who already has filesystem read access to the VPS; at that point they can also read `config.toml` (which holds the bearer tokens and cloud-provider API token in plaintext regardless) and, on a single-tenant personal VPS with no external KMS/HSM, would likely be able to recover whatever key the shim itself would need at startup to decrypt anything anyway. Encrypting the database would add real complexity (key management, migration risk) against a threat model it doesn't actually close off. TLS-in-transit (Phase 2/4) and ordinary OS file permissions on `db.sqlite`/`config.toml` are the controls that actually matter here.

### Admission Validation
`activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim requires it on every CronJob (Phase 5) so the budget guard and deadline enforcement above have something to work with. Rather than silently defaulting it or inventing a bespoke error, a missing deadline is rejected the same way a real cluster's admission webhook/policy would reject a policy violation: HTTP 403, a standard `Status` object (`reason: Forbidden`), and a message in the same shape a tool like Gatekeeper or Kyverno would produce. `src/k8s_status.rs` (Phase 2) builds this response generically, so any future admission/validation rule (not just this one) can reuse it.

### Generic Ephemeral Volumes / Storage Tiers
Scratch storage is declared inline in the CronJob's pod template as a real Kubernetes "generic ephemeral volume" (Phase 5), not a standalone `PersistentVolumeClaim` — it's created and destroyed with the job run that owns it, full stop, so there's no `Pending`/`Bound`/`Released` lifecycle or cross-resource binding to get wrong, and no notion of "retained across runs" at all (see "Future Work" for that case). `spec.storageClassName` selects a performance tier from two hardcoded, internally known names (`kube-shim-standard` default, `kube-shim-fast`), mapped per-provider to that provider's real storage tier (UpCloud: `standard`/`maxiops`). This deliberately avoids two heavier alternatives: real Kubernetes `StorageClass`/`PersistentVolume` dynamic provisioning (a three-resource dance with no practical benefit at this scale), and a portable numeric "target IOPS" field (not actually an established Kubernetes convention — even CSI leaves IOPS-style parameters entirely to the driver — and a poor fit for providers that offer a handful of discrete tiers rather than a continuously dialable number). An unknown class name is rejected via `src/k8s_status.rs`, HTTP 422 `reason: Invalid` — the same idiomatic shape a real cluster uses for "this enum value isn't one of the supported ones," distinct from the 403 `Forbidden` used for the `activeDeadlineSeconds` policy check above (that's a cluster policy denying an otherwise-valid request; this is a plain field-value validation failure).

### Worker VM Network Isolation
Every worker VM (Phase 9) gets UpCloud firewall rules at creation time: all inbound denied except SSH from the shim's own VPS IP, all outbound allowed. Because rule application lags the API call by roughly 1-2 minutes (Phase 7), the reconciliation loop treats this as its own explicit, polled state (`FirewallApplying` → `FirewallVerified`) rather than assuming the API call's success response means the rule is already in effect — nothing starts a container before that transition completes. This achieves "unreachable from the internet, but can still reach it" (Key Constraints) without private networking or a NAT gateway — the VM keeps its normal public IP (needed for outbound egress), it's just unreachable inbound from anyone but the shim, and the shim only trusts that once it's actually verified.

### Workload Abstraction (for future Deployment support)
The reconciliation and VM-provisioning code is written against a `WorkloadKind` enum (Phase 5) rather than assuming "CronJob" directly. Today it has one variant. This costs nothing now but means that when `Deployment` support is added later (see "Future Work"), the shared plumbing — cloud provider client, ephemeral-volume handling, cloud-init templating, log streaming, events, pricing — doesn't need to be reworked; only the state machine for "how a run starts/ends" differs per kind.

### Cloud Provider Abstraction (for future multi-provider support)
All cloud calls go through a `CloudProvider` trait (Phase 7) — `create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `create_firewall_rules`, `get_pricing`, etc. `UpCloudProvider` is the only implementation initially. This is a lightweight seam, not a finished multi-cloud abstraction: its exact method signatures should be expected to change once a second provider (see "Future Work: Additional VPS Providers") is actually implemented against it — Infomaniak's OpenStack API in particular has a materially different shape (Keystone token auth instead of a static bearer token, Cinder volumes, Nova server "flavors", per-project quotas), and it's not worth guessing that shape correctly in advance.
- Dry-run mode for testing (logs API calls, doesn't execute)
- API token/credentials from config file (not K8s Secret, due to bootstrap problem)
- Resource naming (`resource_prefix`-based, Phase 15) for identification and cleanup

### Rolling Budget Model
A token bucket, not a fixed daily reset: `balance` (in `main_currency`) increases by `budget_daily_rate` for every day (fractionally, per reconciliation tick) that passes, capped at `budget_daily_rate × budget_rollover_cap_days`. Spending decrements the balance; a job that would exceed it waits in `BudgetWait` instead of being launched. Example: at CHF 2/day with a 7-day cap, 3 idle days accrue CHF 6 of balance — enough for one job estimated at CHF 5, even though no single day's rate alone would cover it. The underlying provider cost is always computed in EUR (UpCloud's own billing currency) and converted via the ECB's daily reference rate (Phase 13) before being compared against or subtracted from the balance.

### Deployment Model
The shim ships as a `FROM scratch` OCI image (Phase 3): a statically-linked musl binary with embedded TLS roots and no external process dependencies (SSH goes through `russh`, not a subprocess), run rootless via a podman quadlet unit. Persistent state (`db.sqlite`, TLS certs — including the ACME account key and issued certificate, Phase 4 — and `config.toml`) lives on a host bind mount, so a container restart — whether from a crash or an update — never loses job/volume/VM/budget state, and any in-flight job's worker VM is unaffected since it runs independently on the cloud provider. The `kube-shim.brawer.ch` instance updates itself unattended via `podman-auto-update.timer` tracking `:latest` (see Phase 3), a deliberate trade of update-safety for development-loop speed, bounded by UpCloud's prepaid no-auto-recharge billing; a deployment without that backstop should pin a `vX.Y.Z` tag instead and update manually.

### Database Strategy
- SQLite: single file, ACID, no external dependency
- Versioning: update rows by incrementing `version` column
- Reconciliation: poll every 10s as a fallback, woken immediately on specific events via `tokio::sync::Notify` (Phase 6)
- State is single source of truth (SQLite is persistent across shim restarts)

---

## Verification Strategy

### Per-Phase Checklist
- Phase 1: terraform apply/destroy works, resources stored in SQLite
- Phase 2: the authenticated API rejects requests with no/wrong bearer token (401, standard `Status` object, constant-time comparison); multiple tokens can be valid simultaneously (rotation), expired tokens are rejected, and the shim refuses to start with zero valid tokens configured; the public status page (once it exists in Phase 14) stays unauthenticated
- Phase 3: `FROM scratch` image builds and starts (static musl binary, no libc); `podman pull` + `systemctl --user restart` picks up a new version with `db.sqlite` and in-flight jobs unaffected; ghcr.io package requires no pull credentials
- Phase 4: a real hostname gets a CA-trusted certificate with no manual steps; local dev with no hostname configured falls back to self-signed; a cold-start issuance failure makes the shim refuse to start; a renewal failure for an existing valid cert doesn't interrupt service; `:80` serves only the ACME challenge path
- Phase 5: a CronJob's inline ephemeral volume is provisioned/destroyed with the job run; a second differently-sized cronjob coexists without naming collisions; an unknown `storageClassName` is rejected with a 422 `Status` response; a CronJob without `activeDeadlineSeconds` is rejected with a standard Kubernetes 403 `Status` response, surfaced cleanly by both `kubectl` and Terraform
- Phase 6: reconciliation loop advances job states automatically, and does so immediately (not after a 10s delay) for events it's told about directly
- Phase 7: `CloudProvider` trait exists and `UpCloudProvider` is the only caller of it (no direct UpCloud HTTP calls elsewhere); logs show "DRY-RUN" messages, no actual resources created
- Phase 8: small volumes created/deleted cleanly, orphan scan finds/deletes strays
- Phase 9: VMs launch sized per-job, receive cloud-init, containers run only after firewall rules are verified applied; every worker VM is unreachable inbound from outside the shim's own IP but can still reach the internet outbound
- Phase 10: kubectl logs -f works while container running, via `russh` (no `ssh` subprocess spawned by the shim)
- Phase 11: 10+ chaos scenarios (kill shim, container crashes, etc.) with zero orphans and zero duplicate resources; a job that overruns `activeDeadlineSeconds` is force-killed and marked `Failed`/`DeadlineExceeded`, including when the shim was down at the moment the deadline passed
- Phase 12: kubectl describe shows events, kubectl top shows metrics across all running jobs
- Phase 13: costs calculated per job and per family in `main_currency`; a job whose estimate exceeds the budget balance waits in `BudgetWait` and launches once enough has accrued; a job that finishes early returns its unused margin to the balance; the CSV cost report groups correctly by label
- Phase 14: status page and `/healthz`/`/livez`/`/readyz` reachable with no auth on `:443` (same listener/cert as the authenticated API), show events/jobs/cost, never leak secrets, reject all non-GET requests, and don't widen the authenticated API's own auth requirement
- Phase 15: all resources named with the configured `resource_prefix`, cleanup script works, two instances with different prefixes don't interfere with each other's orphan scans
- Phase 16: real osmdiffs job and at least one other cronjob complete successfully, concurrently at least once, cost accurate

### End-to-End Test
```bash
# 1. Provision VPS with provision.sh (installs rootless podman); point DNS
#    at the VPS before first start, so ACME issuance (Phase 4) succeeds
# 2. Deploy shim: podman pull the released image, start via the quadlet unit
# 3. Apply Terraform config with the real cronjob family (osmdiffs + others),
#    using the bearer token from provisioning
# 4. Monitor until jobs complete (6+ hours for osmdiffs)
# 5. Verify:
#    - kubectl logs shows full output for each job
#    - kubectl get pod shows Succeeded status
#    - UpCloud control panel: volumes/VMs created, then deleted
#    - Cost calculated per job and aggregated across the family, in main_currency
#    - Public status page (/ and /statusz) matches the authenticated debug/status output; /healthz, /livez, /readyz all respond with no auth
#    - Shim logs: no errors, all state transitions clean
# 6. Run 2-3 more scheduled cycles (over 1-2 weeks) with zero manual intervention
# 7. Let one real release roll out via podman-auto-update.timer: confirm state
#    and in-flight jobs survive the automatic pull + restart
```

---

## Open Questions / Decisions

All questions below are resolved or deliberately deferred — none block starting Phase 1's already-shipped work or any subsequent phase.

1. **Cloud provider**: resolved — UpCloud is the primary target, not Hetzner. Hetzner discontinued the low-end VPS tier this project's budget depended on; UpCloud's ~€3/month (1 CPU, 1GB RAM, 10GB storage, free IPv4) tier fits, and its REST API was researched directly (not assumed): `Authorization: Bearer <token>` auth, asynchronous server create, synchronous storage create/attach/detach, asynchronous-in-effect firewall rule application (~1-2 min lag), and `GET /1.3/price` returning prices in the account's own billing currency (EUR here). Build against the `CloudProvider` trait (Phase 7) with `UpCloudProvider` as the only implementation, and defer both Hetzner and Infomaniak (OpenStack) to "Future Work" below rather than guessing their shape now.

2. **Container registry auth for *workload* images**: resolved by inference — all of this project's ghcr.io packages are public (open-source work only), so `ghcr.io/brawer/osmdiffs` and the rest of the cronjob family's images are assumed public too, same as the shim's own image (Phase 3). If any workload image later turns out to be private, cloud-init will need registry credentials added — flag that specifically if it comes up, since it wasn't confirmed job-by-job.

3. **Terraform state**: resolved — out of scope for the shim regardless of how Terraform itself is run (locally, or as a GitHub Action). The shim is just an HTTPS server authenticated by a bearer token; where Terraform's own state file lives has no bearing on it.

4. **Monitoring / alerting**: deliberately deferred, not implemented in the initial scope. There's no single established generic webhook standard for arbitrary Kubernetes events — the closest ecosystem convention is Prometheus Alertmanager's webhook receiver (built on metrics + alerting rules) or ad hoc event-forwarders like `eventrouter`, both of which are downstream tooling the cluster operator adds, not something the API server defines. OpenTelemetry itself is a telemetry *data pipeline* (traces/metrics/logs), not an alerting system — alerting is a separate concern layered on top of an OTel-fed backend (Grafana, Prometheus, or a vendor). None of that infrastructure is warranted for a single-VPS personal project. If this gets built later, the lightest-weight fit is a simple outbound webhook POST (a JSON payload) on specific events — job `Failed`/`DeadlineExceeded`, budget balance running low — to a user-configured URL (a Slack incoming webhook, `ntfy.sh`, `healthchecks.io`, etc.), no Prometheus/OTel Collector required. For now: journalctl + the status page only.

5. **RBAC/AuthN**: resolved for the initial scope. Full RBAC is still out of scope (single-user trusted setup), but Phase 2 requires a bearer token — mirroring Kubernetes' own built-in static-token-file authenticator — on every request to the authenticated API, closing the "must stay bound to VPN" gap without needing a VPN or any special networking. The public status page and health endpoints remain deliberately unauthenticated by design (read-only, structurally unable to mutate state) even after they move onto the same `:443` listener as the authenticated API — see Phase 14.

6. **Volume model — reclaim policy vs. tier**: resolved, and revised from the original plan. Implementing full standard Kubernetes dynamic provisioning (a real `StorageClass` + `PersistentVolume` resource pair, with a Retain option) would be meaningfully more work for no practical benefit at this scale and for workloads that are, in practice, always genuinely ephemeral — so the shim uses real Kubernetes **generic ephemeral volumes** (inline in the pod template) instead of standalone PVCs, and repurposes the PVC's `spec.storageClassName` field to select a *performance tier* (`kube-shim-standard`/`kube-shim-fast`) rather than a reclaim policy, since nothing is ever retained across runs at all. See Phase 5 and "Generic Ephemeral Volumes / Storage Tiers" under Key Implementation Details. Standalone `PersistentVolumeClaim` support for data that genuinely needs to survive across runs is deferred — see "Future Work."

7. **Concurrency guard scope**: resolved — dropped entirely, no separate `max_concurrent_jobs`. The rolling budget guard (Phase 13) is the only limiter: at the budget levels this project runs at, it already prevents more than one job's worth of concurrent spend in practice, so a second, separate job-count cap would just be redundant bookkeeping.

8. **Worker VM networking**: resolved for the initial scope. Every worker VM is unreachable from the internet inbound (UpCloud firewall rules permit nothing but SSH from the shim's own VPS IP, verified applied before any container starts — see Phase 9) while remaining free to reach the internet outbound for downloading input data and uploading results. Exposing any of this *inbound*, e.g. running the shim itself as a configurable reverse proxy driven by a Kubernetes `Ingress` resource, is explicitly out of scope for a long time — see "Future Work: Ingress Proxy" below.

9. **Estimated cost accuracy**: resolved. Rather than guessing a default when `activeDeadlineSeconds` is absent, Phase 5 requires it via an admission check (HTTP 403, standard Kubernetes `Status` object, same shape a real admission-policy denial would produce) — so every job always has an explicit, user-chosen worst-case runtime, and Phase 11 force-kills the VM if it's exceeded. No default to get wrong, and the cost estimate is a real bound rather than a hopeful guess.

10. **Bearer token rotation**: resolved. `api_tokens` (Phase 2) is a list, each entry with an optional expiration, checked against all of them — so a new token can be added and adopted by Terraform/kubectl before the old one is removed or allowed to expire, instead of a single atomic cutover with no overlap window. See "Authentication" under Key Implementation Details.

11. **TLS provisioning**: resolved. Rather than a CDN/reverse-proxy in front of the shim handling TLS, or the operator manually managing certificates, the shim manages its own ACME (Let's Encrypt) certificate directly, Caddy-like — see Phase 4. A cold-start issuance failure fails loudly (refuses to start); a renewal failure for an existing valid certificate retries quietly in the background without interrupting service; local dev/CI without a public hostname falls back to the original self-signed behavior.

12. **Secrets-at-rest encryption**: resolved — not implemented. The realistic threat model (an attacker with filesystem access to the VPS) isn't actually closed off by encrypting `db.sqlite`, since `config.toml`'s own bearer/provider tokens are plaintext regardless and there's no external KMS to keep a database encryption key safe from the same attacker. See "Authentication" under Key Implementation Details for the full reasoning.

13. **Reconciliation responsiveness**: resolved. The 10-second poll (Phase 6) stays as a fallback, but specific events (a new job submitted, the budget balance changing) wake the loop immediately via `tokio::sync::Notify`, rather than always waiting up to 10s. See Phase 6 and "Database Strategy" under Key Implementation Details.

14. **CNCF conformance testing**: resolved — not pursued. The [CNCF `k8s-conformance`](https://github.com/cncf/k8s-conformance) program certifies distributions that run the *entire* Sonobuoy/`[Conformance]`-tagged e2e test suite against a real kubelet/controller-manager/scheduler — built for certifying "this is a real, complete Kubernetes," which this project deliberately isn't (a handful of resource types, no scheduler, no kubelet). Pursuing it would be wildly out of proportion to a single-VPS personal project with an intentionally partial API surface. Reliance instead: hewing to the same real Kubernetes API *conventions* already threaded through this plan (`Status` object shapes, admission-denial semantics, discovery endpoints), verified the way every phase above already is — real `kubectl`/Terraform smoke tests against each phase's actual surface, not a conformance certificate.

15. **Naming for multiple concurrent instances**: resolved. `resource_prefix` (default `"kube-shim"`, Phase 5/15) replaces a hardcoded literal in both resource naming and orphan-scan matching, so more than one independent kube-shim instance can run against the same cloud account without collisions or cross-instance interference.

16. **Volume performance tiers / IOPS**: resolved. See item 6 above and "Generic Ephemeral Volumes / Storage Tiers" under Key Implementation Details — `storageClassName` selects from a small, fixed, per-provider-mapped set of tier names rather than a portable numeric IOPS target, since even real Kubernetes/CSI doesn't standardize IOPS as a cross-provider parameter, and these providers offer discrete tiers, not a continuously dialable number.

17. **Public status page port, once ACME exists**: resolved. Once Phase 4 gives kube-shim a real, CA-trusted certificate, keeping the status page on a separate plaintext `:8080` stopped making sense — it moves onto the same `:443` listener as the authenticated API instead (Phase 14), as a structurally separate GET-only sub-router kept outside the bearer-token middleware's scope, rather than a physically separate port. Along the way, added `/healthz`/`/livez`/`/readyz` (genuinely standard Kubernetes API-server health endpoints) and a `/statusz` alias for `/` (the informal gRPC/OpenCensus "zPages" convention, not a literal Kubernetes convention — worth being precise about the difference). A literal Prometheus `/metrics` self-instrumentation endpoint was considered at the same time but deliberately deferred — see "Future Work: Prometheus `/metrics` Self-Instrumentation".

---

## Future Work: Deployments (`apps/v1`)

Not in the initial scope, but the design above is meant to make this additive rather than a rewrite:

- Add `Deployment` (apps/v1) as a second `WorkloadKind` (see Phase 5), alongside `CronJob`.
- Reuses as-is: the cloud provider client, cloud-init templating, log streaming, events/metrics, and pricing — all built for CronJobs.
- New pieces needed: a long-running VM state (create → run → restart-on-crash indefinitely, instead of create → run → delete once), a health-check/restart policy, and — per Open Question 8 — a stable network identity if the deployment needs to be reachable.
- Sequencing: start this only after the CronJob path (Phases 1-16) has run in production for a while and the reconciliation loop has proven itself reliable across restarts. A long-running workload has a much bigger blast radius for a reconciliation-loop bug than a 6-hour job does — a stuck CronJob run wastes at most one job's worth of money; a stuck Deployment could run (and bill) indefinitely.

---

## Future Work: Standalone PersistentVolumeClaim / PersistentVolume

Not in the initial scope. Phase 5 deliberately replaced this plan's original standalone-PVC design with generic ephemeral volumes, since nothing currently in the cronjob family needs data to survive across runs. If that changes (e.g. a job wants to cache a large downloaded dataset — an OSM planet file — instead of re-fetching it every run):

- Add a real `PersistentVolumeClaim`/`PersistentVolume` resource pair alongside (not instead of) generic ephemeral volumes — the two are independent, coexisting Kubernetes concepts; a pod can reference both an ephemeral scratch volume and a bound, retained PVC at once.
- Reclaim policy (`Delete` vs. `Retain`) would live on the new resource, separate from the ephemeral-volume `storageClassName` tier selector introduced in Phase 5 — the two concerns (performance tier, retention) don't need to be conflated just because they both currently ride on the same field name in upstream Kubernetes.
- Sequencing: don't build this speculatively. Wait for an actual job in the family that needs cross-run caching before designing the binding/lifecycle logic — the original PVC design in this plan's early drafts modeled this without a concrete need driving it, which is exactly the premature-generality this revision walked back.

---

## Future Work: Zone / Region Placement (`nodeSelector`)

Not in the initial scope. UpCloud (like most VPS providers) has multiple datacenters/zones, but the shim currently has no way to pin a job to one:

- Real Kubernetes convention for this: the standard `topology.kubernetes.io/region` and `topology.kubernetes.io/zone` node labels, matched via a pod's `nodeSelector` or `nodeAffinity` — there's no broader established convention beyond those two label keys; Kubernetes doesn't mandate specific *values* for them, only that they exist.
- If/when this is needed: honor `nodeSelector` matching those two keys against UpCloud's actual zone identifiers (e.g. `fi-hel1`, `de-fra1`), passed straight through to the `create_server` call. An unmatched/unknown zone would be rejected the same way an unknown `storageClassName` is (HTTP 422, `reason: Invalid`).
- Sequencing: deferred because there's no concrete need yet — the cronjob family doesn't currently care which zone a worker VM lands in, and a single-zone deployment is simpler in every way that matters until that changes.

---

## Future Work: Prometheus `/metrics` Self-Instrumentation

Not in the initial scope — deferred by explicit choice when the status-page/z-pages design was discussed (see Phase 14 and Open Question 17), not because it's hard:

- A literal Prometheus text-exposition endpoint at `/metrics`, unauthenticated, on the same public sub-router as `/healthz`/`/livez`/`/readyz` (Phase 14) — self-instrumentation for the shim process itself (uptime, reconciliation tick count/duration, jobs-by-state gauges, current budget balance, ...).
- Not to be confused with the already-planned, authenticated `metrics.k8s.io` API (Phase 12), which reports job/pod resource usage for `kubectl top` — a different concern (workload metrics) with a different audience (`kubectl`/Terraform, behind the bearer token) than this one (shim self-instrumentation, for an operator's own Prometheus, unauthenticated).
- Straightforward to add later with the `prometheus` (or `metrics`) crate once there's an actual reason to scrape kube-shim itself (e.g. wiring it into existing personal Grafana/Prometheus infra, if any exists) — no design obstacle, just not needed for this round.

---

## Future Work: Additional VPS Providers (Hetzner / Infomaniak / OpenStack)

Not in the initial scope. UpCloud covers the stated budget and constraints on its own; this is here so the `CloudProvider` seam (Phase 7) isn't accidentally designed in a way that forecloses it:

- **Hetzner**: this plan's original primary target, moved here after Hetzner discontinued the low-end VPS tier the original design depended on. No Hetzner-specific code was ever written (the codebase never got past Phase 3 before this revision), so re-adding it later is a clean addition, not an untangling. Hetzner's API shape (static API-key bearer auth, synchronous volume/server operations) is, if anything, simpler than UpCloud's — worth revisiting if UpCloud's pricing/availability/region fit ever changes.
- **Infomaniak / OpenStack**: add an `OpenStackProvider` (or `InfomaniakProvider`, if Infomaniak's API diverges enough from stock OpenStack to need its own logic) implementing the same `CloudProvider` trait. Materially different shape from UpCloud: Keystone token-based auth (tokens expire and need refreshing, unlike UpCloud's static bearer token), Cinder for volumes, Nova for servers ("flavors" instead of server types, plus availability zones and per-project quotas to account for).
- Config would need a `provider: upcloud | hetzner | infomaniak` selector and a per-provider credentials section, rather than the current single implicit UpCloud config block.
- Sequencing and caveat for both: don't start until there's an actual need (capacity/pricing/region no longer fits). Because the `CloudProvider` trait currently has exactly one implementation, its method signatures are a guess, not a validated abstraction — expect to revise the trait itself, not just add a new file, when this work actually begins.

---

## Future Work: kube-shim as an Ingress Proxy

Way out of scope — not sketched in any detail here, just a placeholder so the direction is recorded:

- The idea: let kube-shim itself act as a reverse proxy in front of select worker VMs (or, more likely, future Deployments — see "Future Work: Deployments" above), configured through a standard Kubernetes `Ingress` resource, so specific services could deliberately be made reachable from the internet.
- This directly reverses the network isolation model established in Phase 9 (worker VMs unreachable inbound by design) for whatever is explicitly exposed through it, so whenever this is actually pursued it needs its own security design pass, not an incremental bolt-on: TLS termination (likely reusing the ACME machinery from Phase 4), which `Ingress` fields are even honored, and how it interacts with the existing per-worker-VM firewall rules.
- No `Ingress` (networking.k8s.io/v1) support exists anywhere in the current API surface (Architecture Overview) — this would be new API surface, not a variation on something already planned.

---

## Timeline Estimate

- **Phase 1** (HTTP + CRUD scaffold): 2 days — done
- **Phase 2** (bearer token authentication): 1 day — done
- **Phase 3** (deploy & update mechanism: image, CI, podman quadlet): 2 days — done
- **Phase 4** (automatic TLS via ACME): 1-2 days
- **Phase 5** (generic ephemeral volumes + multi-job resource model + deadline admission check): 1 day
- **Phase 6-7** (reconciliation skeleton + provider trait + dry-run UpCloud): 2 days
- **Phase 8-9** (UpCloud volumes + VMs/containers, incl. async create/firewall handling): 2-3 days
- **Phase 10-11** (log streaming + hardening/deadline enforcement): 2 days
- **Phase 12-13** (events/metrics + pricing/rolling budget guard/cost report/currency conversion): 2-3 days
- **Phase 14** (public status page): 1 day
- **Phase 15** (naming + cleanup): 1 day
- **Phase 16** (multi-job real-workload testing): 2 days
- **Total**: ~4 weeks of development (up from ~3.5 weeks — ACME and the cost-report/currency work are the main additions), 1+ week of running/monitoring across the whole cronjob family. `Deployment` support, standalone PVCs, zone placement, and additional cloud providers (all under "Future Work") are intentionally excluded from this estimate.

---
