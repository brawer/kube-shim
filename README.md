# Kubernetes API Shim

A lightweight Kubernetes API server for running expensive, short-lived containerized workloads on budget infrastructure (Hetzner Cloud, Infomaniak, etc.).

## Overview

This project implements a minimal Kubernetes API compatible server that:
- Runs on a cheap VPS ($10-20/month)
- Accepts Kubernetes CronJob/Secret resources via Terraform
- Manages ephemeral worker VMs and block volumes via Hetzner Cloud API
- Provides cost tracking and log streaming

## Project Structure

```
kube-shim/
├── Cargo.toml              # Rust dependencies
├── src/
│   ├── main.rs            # Server entry point
│   ├── config.rs          # Configuration parsing
│   ├── api/               # Kubernetes API handlers
│   │   ├── mod.rs         # Discovery endpoints
│   │   ├── secret.rs      # Secret CRUD
│   │   └── cronjob.rs     # CronJob CRUD
│   └── db/
│       ├── mod.rs         # Database initialization
│       └── schema.sql     # SQLite schema
├── config-dev.toml        # Development configuration
└── bootstrap/
    └── provision.sh       # VPS provisioning script
```

## Phase 1: Scaffold + HTTP API

**Status**: ✅ **COMPLETE**

**Goal**: Prove Terraform can CRUD resources via the shim.

### What's Implemented

- ✅ Rust project structure with Axum web framework
- ✅ SQLite database with schema for secrets, cronjobs, jobs, events
- ✅ Configuration system (TOML parsing)
- ✅ Kubernetes API types (ObjectMeta, Secret, CronJob)
- ✅ Discovery endpoints (`GET /api/v1`, `GET /apis/batch/v1`)
- ✅ CRUD handlers for Secrets and CronJobs (Create, Read, List, Delete)
- ✅ Basic health check endpoint
- ✅ Development configuration template
- ✅ VPS provisioning script
- ⚠️  TLS support deferred to Phase 2 (using HTTP for Phase 1 MVP)

### Quick Start

1. **Build the project**:
```bash
cd kube-shim
cargo build
```

2. **Run the server**:
```bash
cargo run -- -c config-dev.toml
```

   Server will listen on `http://127.0.0.1:6443`

3. **Test the API** (from another terminal):

```bash
# Create a secret
curl -X POST http://127.0.0.1:6443/api/v1/namespaces/default/secrets \
  -H "Content-Type: application/json" \
  -d '{
    "api_version": "v1",
    "kind": "Secret",
    "metadata": {"name": "test-secret"},
    "data": {"key": "value"}
  }'

# List secrets
curl http://127.0.0.1:6443/api/v1/namespaces/default/secrets

# Get a specific secret
curl http://127.0.0.1:6443/api/v1/namespaces/default/secrets/test-secret

# Delete a secret
curl -X DELETE http://127.0.0.1:6443/api/v1/namespaces/default/secrets/test-secret

# Same operations work for CronJobs at /apis/batch/v1/namespaces/:namespace/cronjobs
```

   Or use the provided test script:
```bash
# Start the server in one terminal
cargo run -- -c config-dev.toml

# In another terminal, run tests
bash test-api.sh
```

4. **Verify database state**:
```bash
sqlite3 db.sqlite
sqlite> SELECT * FROM secrets;
sqlite> SELECT * FROM cronjobs;
sqlite> .quit
```

5. **Health check**:
```bash
curl http://127.0.0.1:6443/health
```

### Testing Checklist (Phase 1)

- [x] Server builds without errors
- [x] Server starts and listens on 127.0.0.1:6443
- [x] Discovery endpoints return correct API resources
- [x] Can create a Secret via HTTP POST
- [x] Can list Secrets via HTTP GET
- [x] Can get a specific Secret
- [x] Can delete a Secret
- [x] Secret data persists in SQLite
- [x] Can create/list/get/delete CronJobs
- [x] Health check endpoint works
- [x] All API responses use proper HTTP status codes (201, 204, 404, etc.)
- [x] CORS layer enabled for browser compatibility

### What's Next (Phase 2)

- Reconciliation loop: 10-second polling for jobs not in terminal state
- Job state machine: Pending → VolumeCreating → VolumeCreated → ...
- Database updates for job status tracking
- Startup recovery logic

## Architecture Notes

### Database Strategy
- SQLite: single file, ACID, no external dependency
- Schema includes tables for secrets, cronjobs, jobs, events, and pricing
- Versioning: each row has a `version` column for concurrency control
- Indices on common queries (namespace, status, timestamps)

### API Design
- Kubernetes-compatible APIs for common operations (CRUD, list)
- Proper HTTP status codes (201 Created, 204 No Content, etc.)
- JSON serialization of all data structures
- Namespace isolation built-in

### Configuration
- TOML-based configuration file
- Development config template provided (config-dev.toml)
- Support for multiple deployment environments

## Development

### Running Tests

```bash
cargo test
```

### Building for Production

```bash
cargo build --release
```

The binary will be at `target/release/kube-shim`.

## VPS Deployment

Use the provisioning script to set up a fresh VPS:

```bash
bash bootstrap/provision.sh
```

This will:
- Update system packages
- Install Rust
- Create application directory
- Generate self-signed TLS certificates
- Set up systemd service file
- Create database directory

Then deploy your binary and config, and start the service:

```bash
systemctl start kube-shim
journalctl -u kube-shim -f
```
