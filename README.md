# Kubernetes API Shim

A lightweight Kubernetes-API-compatible server for running expensive, short-lived containerized batch workloads on budget VPS infrastructure.

## Overview

This project runs on a cheap VPS (~€3-20/month) and:
- Accepts Kubernetes `Secret`/`CronJob` manifests via Terraform's `kubernetes` provider, authenticated by a bearer token — no VPN or special networking required
- Creates ephemeral UpCloud Cloud Server instances per job run, sized per-job from the CronJob pod template's resource requests
- Provisions per-job scratch storage as an inline generic ephemeral volume, the same way real Kubernetes does
- Runs containers via cloud-init + rootless Podman on worker VMs that are unreachable from the internet inbound while still reaching it outbound
- Serves its own TLS certificate via ACME (Let's Encrypt), falling back to self-signed when no public hostname is configured
- Streams logs over SSH, tracks cost, and enforces a rolling budget

The design talks to the cloud through a small provider-agnostic interface; UpCloud is the only implementation today, with others (Hetzner, Infomaniak) plannable later without reworking the reconciliation loop.

## Status

**Phases 1-11 of 16 are complete** — real UpCloud VM/volume provisioning, ACME TLS, bearer-token auth, cloud-init-based container execution, SSH-based log streaming and exit-code capture, and reconciliation hardening (idempotent retries, `activeDeadlineSeconds`/timeout enforcement) are all built and running in production. Remaining: Events/Metrics APIs, pricing + budget guard, public status page, resource naming/cleanup polish, and final testing/hardening.

**[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) is the living source of truth for project status** — this README intentionally doesn't duplicate a per-phase changelog, since that goes stale the moment the next phase ships. It has, per phase: what was planned vs. what was actually built (including deviations and real findings discovered along the way), the full state-machine diagram, every config option, and a "Critical Files Summary" mapping each source file to the phase(s) that touched it.

## Project Structure

```
kube-shim/
├── Cargo.toml              # Rust dependencies
├── src/
│   ├── main.rs             # Server entry point, both listeners (:443, :80)
│   ├── config.rs           # Configuration parsing
│   ├── api/                # Kubernetes API handlers (secrets, cronjobs, pod logs)
│   ├── reconcile/          # Job state machine + reconciliation loop
│   ├── providers/          # CloudProvider trait + UpCloud implementation
│   ├── ssh.rs              # SSH client to worker VMs (russh, no subprocess)
│   └── db/
│       ├── mod.rs          # Database initialization
│       └── schema.sql      # SQLite schema
├── bootstrap/
│   ├── provision.sh            # One-time VPS setup (podman, service user, TLS, config.toml)
│   └── cloud-init-template.sh  # Worker VM bootstrap (volume mount, container run)
├── deploy/kube-shim.container  # Podman quadlet unit for running the shim itself
├── Containerfile                # FROM scratch release image build
└── config-dev.toml              # Local development configuration
```

See `docs/IMPLEMENTATION_PLAN.md`'s own "Critical Files Summary" for the complete, maintained file-by-file map.

## Quick Start (local development)

1. **Build the project**:
```bash
cargo build
```

2. **Generate a throwaway self-signed cert** (only needed once — `config-dev.toml` points at `certs/cert.pem`/`certs/key.pem`, which aren't generated automatically):
```bash
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
```

3. **Run the server**:
```bash
cargo run -- -c config-dev.toml
```

   `config-dev.toml` runs with `upcloud.dry_run = true` (logs what it would do, never calls the real UpCloud API) — the server listens on `https://127.0.0.1:6443`.

4. **Test the API** (from another terminal, `-k` because of the self-signed dev cert):

```bash
TOKEN="dev-only-insecure-token"  # matches config-dev.toml

# Health check
curl -k -H "Authorization: Bearer $TOKEN" https://127.0.0.1:6443/health

# Create a CronJob (activeDeadlineSeconds is required by admission policy)
curl -k -X POST https://127.0.0.1:6443/apis/batch/v1/namespaces/default/cronjobs \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "api_version": "batch/v1",
    "kind": "CronJob",
    "metadata": {"name": "demo", "namespace": "default"},
    "spec": {
      "schedule": "0 2 * * *",
      "jobTemplate": {"spec": {
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{"name": "x", "image": "busybox:latest"}]}}
      }}
    }
  }'

# List CronJobs
curl -k -H "Authorization: Bearer $TOKEN" https://127.0.0.1:6443/apis/batch/v1/namespaces/default/cronjobs

# Same CRUD shape works for Secrets at /api/v1/namespaces/:namespace/secrets
```

   Or use the provided scripts:
```bash
bash test-api.sh        # exercises the API end-to-end against a running dev server
bash smoke-test.sh       # starts its own throwaway server + cert, runs auth/TLS/discovery checks
```

5. **Inspect database state**:
```bash
sqlite3 db.sqlite
sqlite> SELECT * FROM jobs;
sqlite> .quit
```

## Development

### Running Tests

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

### Building for Production

```bash
cargo build --release
```

Production builds are a `FROM scratch`, statically-linked musl binary built via `Containerfile` in CI — see `.github/workflows/release-build.yml`.

## VPS Deployment

`bootstrap/provision.sh` (run once, as root, on a fresh VPS) installs rootless podman, creates a dedicated unprivileged service user, generates a self-signed cert and starter `config.toml`, and prints the remaining manual steps — in short:

```bash
# After provision.sh finishes:
# 1. Edit the generated config.toml: set your real upcloud.token, and
#    (once DNS is pointed here) uncomment `hostname` to enable ACME.
# 2. Copy deploy/kube-shim.container into the service user's quadlet dir.
# 3. As that service user (provision.sh prints the exact commands):
systemctl --user daemon-reload
systemctl --user start kube-shim.service
journalctl --user -u kube-shim.service -f
```

The shim ships as a prebuilt OCI image (`ghcr.io/brawer/kube-shim`); updating a running instance is `podman pull` + `systemctl --user restart kube-shim.service`, or fully automatic via `podman-auto-update.timer`. See [`docs/RELEASING.md`](docs/RELEASING.md) for how releases are cut and published.
