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

### Phase 4: Automatic TLS via ACME (Days 4-5)
**Goal:** Replace Phase 1's self-signed bootstrap certificate with a real, automatically issued and renewed TLS certificate — Caddy-like: point a hostname at the shim and it manages its own valid certificate, no manual cert copying, no CDN required in front of it.

**Deliverables:**
- The authenticated Kubernetes API moves from `:6443` to standard `:443`, and a new minimal responder listens on `:80` for Let's Encrypt's HTTP-01 challenge — the two ports Caddy itself uses, and the two ports a plain hostname (`https://kube-shim.brawer.ch/`, no `:6443` in the URL) needs. This reverses Phase 3's original "both ports >1024, so rootless podman needs no special capability" choice; whether rootless podman can actually publish `:443`/`:80` without extra capabilities (`net.ipv4.ip_unprivileged_port_start`, or `podman run --cap-add`/quadlet's `AddCapability=`) needs to be verified hands-on against the real target distribution, the same way Phase 3's own podman/rootless mechanics were verified directly rather than assumed.
- ACME client logic (issuance + automatic renewal) built on `rustls-acme` or equivalent, integrated with the existing `axum-server`/`rustls` TLS setup from Phase 1/3 rather than replacing it — `rustls-acme` in particular is designed to plug into exactly that stack (hands the TLS acceptor a certificate resolver that swaps in fresh certs on renewal, no listener restart needed).
- `:80` serves *only* `/.well-known/acme-challenge/{token}` — the same "explicit allowlist, not just an auth gate" pattern later used for the public status page/health endpoints on `:443` (Phase 14): every other path 404s, by construction, not by convention.
- Config: `hostname` (the real public DNS name to request a certificate for) and `acme_directory` (`staging` | `production`, or a literal ACME directory URL) — staging uses Let's Encrypt's staging environment (higher rate limits, a certificate chain that isn't publicly trusted) so development/testing never risks tripping the production environment's real rate limits. `config-dev.toml` defaults to `staging`; the real deployment's config uses `production`.
- **Cold start vs. renewal failure handled differently, on purpose:** if the shim has no cached certificate yet (first boot, or a wiped `/data`) and ACME issuance fails, it retries a bounded number of times with backoff and then **fails to start** rather than serving broken or absent TLS — an operator needs to know immediately that DNS/networking/rate-limits are misconfigured, not discover it when a client's TLS handshake mysteriously fails. A **renewal** failure for a certificate that's still valid, by contrast, logs the failure and keeps retrying quietly in the background on its normal schedule, continuing to serve the still-valid certificate in the meantime — a transient Let's Encrypt outage or rate-limit bump shouldn't cause a self-inflicted outage of an otherwise-healthy service.
- **Local dev/CI fallback:** when no `hostname` is configured (or it resolves to `localhost`/a private address), ACME is skipped entirely and the shim falls back to generating a self-signed certificate for that hostname — the same behavior Phase 1 already shipped, now demoted to "the fallback" rather than "the only option."
- `deploy/kube-shim.container` updated: `PublishPort=443:443` and `PublishPort=80:80` replace the old `:6443` mapping. `:8080` is left in place for now (it isn't wired to anything real yet, since the status page doesn't exist as code until Phase 14) — Phase 14 removes it once the status page/health endpoints turn out not to need a port of their own either.

**Files to create/modify:**
- `src/tls.rs` - extend `load_tls_config()` with ACME issuance/renewal via `rustls-acme` (or equivalent), keeping the existing self-signed path as the no-hostname-configured fallback
- `src/acme_challenge.rs` (new) - the minimal `:80` HTTP-01 challenge-only router
- `src/main.rs` - start the `:443` (was `:6443`) and new `:80` listeners
- `src/config.rs` - add `hostname: Option<String>`, `acme_directory: AcmeDirectory` (staging/production/custom URL)
- `deploy/kube-shim.container` - port publishing updated to `443`/`80`
- `bootstrap/provision.sh` - drop self-signed cert generation from the real-deployment path (still used as the code-level fallback, just no longer provisioned up front); document that DNS must point at the VPS before first start

**Testing:**
```bash
# Local dev, no hostname configured: falls back to self-signed, exactly
# as Phase 1 already behaved
cargo run -- -c config-dev.toml
curl -k https://localhost:443/api/v1

# Real hostname, staging directory (safe to repeat without hitting
# production rate limits):
# config.toml: hostname = "kube-shim.brawer.ch", acme_directory = "staging"
# Confirm a certificate is issued, chain is the (untrusted) LE staging chain
curl --cacert <(curl -s https://letsencrypt.org/certs/staging/letsencrypt-stg-root-x1.pem) \
  https://kube-shim.brawer.ch/api/v1

# Cold-start failure: point hostname at a non-existent/unreachable DNS
# name and confirm the shim logs the failure and exits non-zero, rather
# than starting with no/broken TLS

# Renewal-failure resilience: force a renewal attempt to fail (e.g. block
# outbound to the ACME directory briefly) while an existing valid cert is
# in place; confirm the shim keeps serving the existing cert and retries
# in the background instead of restarting or refusing connections
```

---

### Phase 5: Generic Ephemeral Volumes + Multi-Job Resource Model (Days 5-6)
**Goal:** Let Terraform provision per-job scratch storage the same way real Kubernetes does for genuinely ephemeral storage — inline in the pod template — and remove the hardcoded single-workload assumptions from Phase 1 before the reconciliation loop is built on top of them.

**Deliverables:**
- Support for the CronJob pod template's `volumes: - name: scratch ephemeral: volumeClaimTemplate: spec: {resources.requests.storage, storageClassName}` field — real Kubernetes' "generic ephemeral volumes" (GA since 1.23) — instead of a standalone `PersistentVolumeClaim` resource type with its own CRUD handlers and a separate binding step. Simpler than this plan's original PVC-based design (see GitHub issue #16) and a better match for reality: nothing built here needs to survive across job runs, so there's no `Pending`/`Bound`/`Released` lifecycle, no cross-resource lookup, and no orphaned-claim bookkeeping to get wrong. Standalone `PersistentVolumeClaim`/`PersistentVolume` support for a job that *does* want to cache data across runs is deferred — see "Future Work" below.
- `spec.storageClassName` is repurposed from the original plan: since nothing is ever retained, it no longer selects a reclaim policy. Instead it selects a **performance tier** — two built-in, hardcoded class names, `kube-shim-standard` (the default when omitted) and `kube-shim-fast` — mapped internally, per cloud provider, to that provider's closest matching storage tier (UpCloud: `standard` / `maxiops`). No real `StorageClass` resource or general CSI-style pluggable parameters: providers in this space (UpCloud, and likely Infomaniak/Hetzner too) offer a small number of discrete storage tiers, not a continuously tunable IOPS number, and even real Kubernetes doesn't standardize IOPS as a portable parameter — that's left entirely to whichever CSI driver is bound. A small, fixed, hardcoded per-provider lookup table (`match classname { ... }` inside each `CloudProvider` implementation) is both simpler and more honest about what these providers actually offer than inventing a numeric abstraction they can't precisely honor. An unknown class name is rejected the standard way: HTTP 422, `Status` object, `reason: Invalid`, `causes: [{reason: FieldValueNotSupported, field: "...storageClassName"}]` — built via `src/k8s_status.rs` (Phase 2), same as every other structured error in this API.
- Database: `job_volumes` table (job_id, size_gb, storage_class_name, provider_volume_id, mount_point) — one row per job *run*, not a durable row reused across runs, since nothing here persists between runs.
- Per-job VM sizing: read `resources.requests.cpu` / `.memory` from the CronJob's pod template and map to a cloud-provider server size (small lookup table), instead of a fixed single size — needed now that the shim runs more than one workload shape.
- Config: `resource_prefix` (default `"kube-shim"`), reserved here since this is the first phase that gives cloud resources shim-managed names — actually threaded through naming and orphan-scan matching in Phase 15. Also reserves the rolling-budget parameters (`budget_daily_rate`, `budget_rollover_cap_days`, in `main_currency` — see Phase 13) and `main_currency` itself — this just reserves the config shape; the accrual/enforcement/conversion logic is built in Phase 13, once cost calculation and ECB-rate sync exist.
- Internal `WorkloadKind` enum (`CronJob` for now) threaded through the reconciliation types, so the Phase 8+ VM-provisioning and volume-handling code isn't written in a way that assumes "CronJob" is the only possible workload kind. This is purely an internal abstraction — no new API surface — done now so the future Deployment support (see "Future Work") doesn't require rewriting this layer.
- **Admission check on CronJob create/update: `spec.jobTemplate.spec.activeDeadlineSeconds` must be set.** `activeDeadlineSeconds` is optional in the real Kubernetes API, but the shim needs a hard worst-case runtime bound for every job to make the budget guard (Phase 13) and deadline enforcement (Phase 11) meaningful, so it requires it via policy the same way a real cluster's `ValidatingAdmissionPolicy`/webhook would. A CronJob submitted without it is rejected with the same response shape a real admission webhook denial produces: HTTP 403, a `Status` object (`kind: Status`, `reason: Forbidden`, built via Phase 2's `src/k8s_status.rs`), message `admission webhook "kube-shim.io/require-active-deadline" denied the request: spec.jobTemplate.spec.activeDeadlineSeconds must be set (bounds the job's worst-case cost against the budget guard)`. `kubectl`/Terraform surface this exactly like any real admission denial — resolves Open Question 9.

**Files to create/modify:**
- `src/api/cronjob.rs` - parse/validate the pod template's `ephemeral` volume field; add the `activeDeadlineSeconds` admission check on create/update
- `src/admission.rs` (new) - the `activeDeadlineSeconds` policy check itself, reusing Phase 2's `src/k8s_status.rs` for the response
- `src/volumes.rs` (new) - `storageClassName` → provider-tier lookup, shared across providers
- `src/db/schema.sql` - add `job_volumes` table
- `src/config.rs` - add `resource_prefix`, `main_currency`, `budget_daily_rate`, `budget_rollover_cap_days`
- `src/workload.rs` (new) - `WorkloadKind` enum + shared resource-sizing helpers

**Testing:**
```bash
terraform apply   # CronJob's pod template now includes an inline ephemeral volume
kubectl get pod osmdiffs-weekly-... -o yaml   # shows the ephemeral volume spec
sqlite3 db.sqlite "SELECT job_id, size_gb, storage_class_name FROM job_volumes"
# Apply a second, differently-sized cronjob and confirm both are stored
# independently with no naming collisions.

# Storage tier:
kubectl apply -f cronjob-with-unknown-storage-class.yaml
# Error from server (Invalid): ...storageClassName: Unsupported value...
# Apply one with storageClassName: kube-shim-fast and confirm it's accepted

# Admission check:
kubectl apply -f cronjob-no-deadline.yaml
# Error from server (Forbidden): error when creating "cronjob-no-deadline.yaml":
# admission webhook "kube-shim.io/require-active-deadline" denied the request: ...
terraform apply   # same manifest via Terraform: apply fails with the same message, cleanly
```

---

### Phase 6: Reconciliation Loop Skeleton (Days 6-7)
**Goal:** Build the state machine that will drive all orchestration.

**Deliverables:**
- Reconciliation loop: `tokio::time::interval` every 10 seconds as the **fallback** cadence — not the only trigger. A `tokio::sync::Notify` lets specific events wake the loop immediately instead of waiting up to 10s: a new job submitted via the API, and (once Phase 13 exists) the budget balance crossing a job out of `BudgetWait`. The 10s poll stays in place regardless, since it's still what catches external state changes the shim wouldn't otherwise hear about (e.g. a provider-side VM failure) — the `Notify` is purely a responsiveness improvement for the cases the shim already knows about immediately, not a replacement for polling.
- Job state machine: defined states + transition logic
- Database updates for job status, volume_id, worker_vm_id, etc.
- Error handling + retry tracking per job
- Startup reconciliation (detect orphaned jobs from crashed shim)

**Files to create/modify:**
- `src/reconcile/mod.rs` - reconciliation loop entry point, `Notify`-based wake-up alongside the fallback interval
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
# Watch logs to see reconciliation loop detect it and advance state --
# it should fire within milliseconds of the API call, not wait for the
# 10s fallback tick
journalctl -u kube-shim -f
# Verify state progression in SQLite
sqlite3 db.sqlite "SELECT name, status, retry_count FROM jobs"
```

---

### Phase 7: Cloud Provider Integration (Dry-Run Mode) (Days 7-8)
**Goal:** Call the real UpCloud API but don't actually create resources yet — and define the provider interface so UpCloud isn't hardcoded throughout the codebase.

**Deliverables:**
- A `CloudProvider` trait (`create_volume`, `delete_volume`, `attach_volume`, `create_server`, `delete_server`, `create_firewall_rules`, `get_pricing`, ...) defined *before* writing any UpCloud-specific code. `create_firewall_rules` is what Phase 9 uses to lock worker VMs down to no inbound traffic except from the shim itself. `UpCloudProvider` is the only implementation for now. This is what lets Hetzner and Infomaniak (OpenStack) be added later as additional implementations instead of a rewrite (see "Future Work") — but it's a lightweight seam, not a finished multi-cloud abstraction; expect its exact method signatures to need adjustment once a second provider is actually implemented against it.
- UpCloud client: no official Rust SDK exists, so this is a hand-rolled REST client on `reqwest` (already a dependency) rather than a provider-specific crate like the originally-planned `hcloud`. UpCloud's API (`https://api.upcloud.com/1.3/...`) uses `Authorization: Bearer <token>` the same shape as this project's own bearer-token auth (Phase 2), which simplifies the config story: `[upcloud] token = "..."` alongside the same kind of secret already handled elsewhere.
- **UpCloud's API has real asynchrony/timing quirks the reconciliation loop needs to model explicitly, discovered from its own API reference rather than assumed:**
  - `POST /1.3/server` (create) is **asynchronous** — the response returns before the server is actually ready, so a `VMCreating` → `VMRunning` transition needs an explicit poll/wait step, not a synchronous "create returns a ready server" assumption.
  - `POST /1.3/storage` (create volume) is synchronous; attach/detach go through their own dedicated endpoints and are also synchronous, and can run against a live (already-running) server.
  - Firewall rules (`POST`/`PUT /1.3/server/{uuid}/firewall_rule`) are **asynchronous in effect**: the API call returns immediately, but the rule takes roughly 1-2 minutes to actually apply. This matters for a security-relevant guarantee ("worker VM is unreachable inbound") — the reconciliation loop must not consider a worker VM's network isolation established the instant the firewall API call returns; it needs its own explicit wait/verify step before the VM is treated as safe to leave running unattended (see Phase 9).
  - `GET /1.3/price` returns prices in the account's own billing currency — for this project's UpCloud account, that's EUR (UpCloud doesn't offer CHF billing), which is exactly why Phase 13's `main_currency` conversion exists.
- `DRY_RUN=true` config flag
- Reconciliation step: `VolumePending` → attempt volume creation (logged, not executed)
- Reconciliation step: `VMPending` → attempt VM creation (logged, not executed)
- Error handling for UpCloud API authentication failures

**Files to modify:**
- `src/providers/mod.rs` (new) - `CloudProvider` trait
- `src/providers/upcloud/mod.rs` (new) - UpCloud REST client wrapper implementing the trait, built on `reqwest`
- `src/providers/upcloud/volumes.rs` (new) - volume (storage) operations
- `src/providers/upcloud/servers.rs` (new) - server operations, incl. the create-then-poll pattern
- `src/reconcile/job.rs` - add DRY_RUN checks before API calls
- `src/config.rs` - add dry_run boolean, `[upcloud] token`
- `config-dev.toml` - set dry_run = true

**Testing:**
```bash
# Apply osmdiffs CronJob (small test version, 1GB volumes)
terraform apply
# Watch logs:
# "DRY-RUN: Would create volume kube-shim-vol-osmdiffs-weekly-20260915-abc size=1GB"
# "DRY-RUN: Would launch VM kube-shim-worker-osmdiffs-weekly-20260915-abc"
# Verify NO resources created in UpCloud's control panel
```

---

### Phase 8: Real UpCloud Operations (Small Volumes) (Days 8-9)
**Goal:** Actually provision volumes, but small (1-10GB) for safety.

**Deliverables:**
- Set `dry_run = false` in config
- Real volume (storage) creation in the reconciliation loop, at the size and tier (`kube-shim-standard`/`kube-shim-fast`, Phase 5) the job's ephemeral volume spec requests
- Volume attachment to the worker VM
- Error handling + retry logic with timeouts (5 min for volume creation)
- Orphan detection: scan UpCloud for storage/servers with our `resource_prefix`, delete if not tracked in DB
- Database: track volume_id, volume_device, mount_point

**Files to modify:**
- `src/providers/upcloud/volumes.rs` - implement real create/attach/detach/delete
- `src/reconcile/job.rs` - implement volume state steps (VolumeCreating, VolumeAttaching, VolumeAttached)
- `src/reconcile/orphan_scan.rs` (new) - periodic orphan detection + cleanup
- `src/db/schema.sql` - add volume_device, mount_point columns

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
- `src/providers/upcloud/servers.rs` - implement real server create/delete + the create→poll-until-running pattern
- `src/providers/upcloud/firewall.rs` (new) - creates the restrictive firewall ruleset for every worker VM, polls until applied
- `src/cloud_init.rs` (new) - generate cloud-init script with proper escaping
- `src/reconcile/job.rs` - add VM state steps (VMCreating, VMRunning, FirewallApplying, FirewallVerified, etc.)
- `bootstrap/cloud-init-template.sh` (new) - bash template for VM startup
- `src/db/schema.sql` - add worker_vm_id, worker_ssh_ip, worker_vm_name, exit_code columns

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
| `src/tls.rs` | TLS config: self-signed bootstrap (Phase 1), ACME issuance/renewal (Phase 4) | Phases 1, 4 |
| `src/acme_challenge.rs` | `:80` HTTP-01 challenge-only responder | Create (Phase 4) |
| `src/api/*.rs` | Kubernetes API handlers | Phases 1, 5, 12 |
| `src/volumes.rs` | `storageClassName` → provider storage-tier lookup | Create (Phase 5) |
| `src/workload.rs` | `WorkloadKind` abstraction (CronJob now, Deployment later) | Create (Phase 5) |
| `src/admission.rs` | `activeDeadlineSeconds`-required policy check | Create (Phase 5) |
| `src/db/schema.sql` | SQLite schema | Phases 1, 5-6, 8-9, 11-13 |
| `src/reconcile/*.rs` | State machine loop, `Notify`-based wake-up, `activeDeadlineSeconds` enforcement | Create (Phases 6, 8-9, 11, 13) |
| `src/providers/*.rs` | `CloudProvider` trait + UpCloud implementation | Create (Phases 7-9, 13) |
| `src/providers/upcloud/firewall.rs` | Firewall rules for worker VMs (create + poll-until-applied) | Create (Phase 9) |
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
rustls-acme = "0.13"      # Phase 4: ACME issuance/renewal against axum-server's rustls stack
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
