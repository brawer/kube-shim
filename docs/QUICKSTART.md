# Kube-Shim Quick Start Guide

## What Was Built (Phase 1)

A working Kubernetes API-compatible server with:
- ✅ HTTP API endpoints for managing Secrets and CronJobs
- ✅ SQLite database for persistence
- ✅ Proper Kubernetes API structure
- ✅ Full CRUD operations (Create, Read, List, Delete)

## 60-Second Start

### 1. Build
```bash
cargo build
```

### 2. Run
```bash
cargo run -- -c config-dev.toml
```

You should see:
```
Server listening on http://127.0.0.1:6443 (Note: Phase 2 will add TLS support)
```

### 3. Test (in another terminal)
```bash
# Health check
curl http://127.0.0.1:6443/health

# Create a secret
curl -X POST http://127.0.0.1:6443/api/v1/namespaces/default/secrets \
  -H "Content-Type: application/json" \
  -d '{"api_version":"v1","kind":"Secret","metadata":{"name":"my-secret"},"data":{"key":"value"}}'

# List secrets
curl http://127.0.0.1:6443/api/v1/namespaces/default/secrets
```

## API Endpoints

### Secrets (v1 API)
- `POST /api/v1/namespaces/{namespace}/secrets` - Create secret
- `GET /api/v1/namespaces/{namespace}/secrets` - List all
- `GET /api/v1/namespaces/{namespace}/secrets/{name}` - Get one
- `DELETE /api/v1/namespaces/{namespace}/secrets/{name}` - Delete

### CronJobs (batch/v1 API)
- `POST /apis/batch/v1/namespaces/{namespace}/cronjobs` - Create cronjob
- `GET /apis/batch/v1/namespaces/{namespace}/cronjobs` - List all
- `GET /apis/batch/v1/namespaces/{namespace}/cronjobs/{name}` - Get one
- `DELETE /apis/batch/v1/namespaces/{namespace}/cronjobs/{name}` - Delete

### Discovery
- `GET /api/v1` - List v1 resources
- `GET /apis/batch/v1` - List batch/v1 resources
- `GET /health` - Health check

## Database

Check what's stored:
```bash
sqlite3 db.sqlite
SELECT * FROM secrets;
SELECT * FROM cronjobs;
.quit
```

## Run Full Test Suite

```bash
bash test-api.sh
```

This will:
- Check health
- Test secret CRUD
- Test CronJob CRUD
- Verify database state
- Clean up test data

## File Locations

- **Binary**: `target/debug/kube-shim` (or `release/kube-shim` if built with `--release`)
- **Database**: `db.sqlite` (created on first run)
- **Config**: `config-dev.toml` (customize as needed)
- **Log output**: Console (set RUST_LOG for filtering)

## Useful Commands

```bash
# Clean rebuild
cargo clean && cargo build

# Release build (optimized)
cargo build --release

# Run with verbose logging
RUST_LOG=debug cargo run -- -c config-dev.toml

# Run tests
cargo test

# Type check without building
cargo check

# View database directly
sqlite3 db.sqlite ".tables"
sqlite3 db.sqlite ".schema"
```

## What's Next?

See `PHASE_1_COMPLETION.md` for detailed completion report.

See `/root/.claude/plans/imperative-juggling-snowflake.md` for full implementation plan.

**Phase 2**: Reconciliation loop (state machine for job orchestration)  
**Phase 3-4**: Hetzner Cloud integration  
**Phase 5**: VM provisioning and container execution  

## Troubleshooting

### "Address already in use"
Another process is using port 6443. Either:
- Kill the other process: `lsof -i :6443 | grep LISTEN | awk '{print $2}' | xargs kill`
- Change port in `config-dev.toml`

### Database errors
Delete the old database and start fresh:
```bash
rm db.sqlite
cargo run -- -c config-dev.toml
```

### Compilation errors
Ensure you have Rust installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Then retry: `cargo build`

## Next Steps

1. Read `PHASE_1_COMPLETION.md` for full details
2. Read the plan at `/root/.claude/plans/imperative-juggling-snowflake.md`
3. Proceed to Phase 2 implementation (reconciliation loop)
