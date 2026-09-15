# Testing Strategy for Kube-Shim

This document outlines the comprehensive testing approach for all 11 implementation phases.

## Testing Pyramid

```
         Integration Tests (E2E scenarios)
              API endpoint tests
           Database + business logic
        Unit tests (functions, modules)
```

## Test Types

### 1. Unit Tests
- **Location**: Within source files (e.g., `#[cfg(test)] mod tests`)
- **Focus**: Individual functions, validation logic, serialization
- **Tools**: Built-in Rust test framework
- **Coverage**: Error cases, edge cases, happy paths

### 2. Integration Tests
- **Location**: `tests/` directory
- **Focus**: API endpoints, database operations, state transitions
- **Tools**: Axum test utilities, SQLx query testing
- **Coverage**: Full request/response cycles, database persistence

### 3. End-to-End Tests
- **Location**: Bash scripts in `scripts/` (added per phase)
- **Focus**: Real server startup, actual API calls, database state
- **Tools**: curl, sqlite3, jq
- **Coverage**: Full workflows, timing, cleanup

## Running Tests

### All Tests
```bash
cargo test
```

### Only Unit Tests
```bash
cargo test --lib
```

### Only Integration Tests
```bash
cargo test --test '*'
```

### Specific Test
```bash
cargo test test_create_and_get_secret
```

### With Output
```bash
cargo test -- --nocapture
```

### With Backtrace
```bash
RUST_BACKTRACE=1 cargo test
```

## Phase-by-Phase Testing

### Phase 1: Scaffold + HTTP API ✅

**Unit Tests**:
- ✅ Config parsing (TOML validation)
- ✅ Database initialization
- ✅ Secret/CronJob serialization
- ✅ API discovery response format

**Integration Tests**:
- ✅ Create/read/list/delete secrets (database persistence)
- ✅ Create/read/list/delete cronjobs (database persistence)
- ✅ Namespace isolation
- ✅ HTTP status codes (201, 204, 404, 500)
- ✅ Concurrent requests

**E2E Tests**:
- ✅ Server startup
- ✅ API endpoint accessibility
- ✅ Database file creation
- Manual test script: `test-api.sh`

**Coverage Target**: 80%+ for API handlers, 100% for core logic

---

### Phase 2: Reconciliation Loop Skeleton

**New Tests**:
- Job status state machine transitions
- Retry logic (exponential backoff)
- Startup recovery (orphan detection logic)
- Database transaction isolation
- Mock reconciliation cycles (no external APIs)

**Test Scenarios**:
```rust
#[test]
fn test_state_transition_created_to_volume_pending() { }

#[test]
fn test_retry_with_exponential_backoff() { }

#[test]
async fn test_startup_recovery_finds_orphaned_jobs() { }

#[test]
fn test_max_retry_limit_moves_to_failed() { }
```

**E2E Testing**:
```bash
# Simulate reconciliation cycle
cargo run & 
sleep 1
curl -X POST http://localhost:6443/api/v1/namespaces/default/secrets \
  -H "Content-Type: application/json" -d '{...}'
sleep 15  # Wait for reconciliation
sqlite3 db.sqlite "SELECT status FROM jobs WHERE name = 'test';"
```

---

### Phase 3: Hetzner Integration (Dry-Run)

**New Tests**:
- Hetzner client initialization
- API token validation
- Dry-run mode logging (verify no actual API calls)
- Error handling for invalid credentials

**Test Scenarios**:
```rust
#[test]
async fn test_hetzner_dry_run_mode() { }

#[test]
async fn test_hetzner_auth_failure_handling() { }

#[test]
fn test_dry_run_logging_format() { }
```

**E2E Testing**:
```bash
# With DRY_RUN=true, verify no resources created
RUST_LOG=debug cargo run -- -c config-dev.toml 2>&1 | grep "DRY-RUN"
```

---

### Phase 4: Real Hetzner Operations

**New Tests**:
- Volume creation/deletion
- Volume attachment/detachment
- Orphan detection and cleanup
- Resource naming convention validation

**Test Scenarios**:
```rust
#[test]
async fn test_volume_create_and_attach() { }

#[test]
async fn test_orphan_volume_detection() { }

#[test]
async fn test_volume_cleanup_on_failure() { }

#[test]
fn test_resource_naming_convention() { }
```

**Safety Tests**:
- Start with 1GB volumes (not 250GB)
- Verify cleanup after each test
- Check Hetzner console manually
- Run 5+ cycles with zero orphans

---

### Phase 5: VM Provisioning

**New Tests**:
- Cloud-init script generation
- VM creation/deletion
- SSH connectivity (mocked)
- Container execution simulation

**Test Scenarios**:
```rust
#[test]
fn test_cloud_init_script_generation() { }

#[test]
async fn test_vm_startup_sequence() { }

#[test]
async fn test_container_execution_tracking() { }
```

---

### Phase 6: Log Streaming

**New Tests**:
- SSH connection pooling
- Docker logs streaming
- Fallback to cached logs
- SSH disconnect handling

**Test Scenarios**:
```rust
#[test]
async fn test_log_streaming_from_container() { }

#[test]
async fn test_cached_logs_fallback() { }

#[test]
async fn test_ssh_reconnection_after_failure() { }
```

---

### Phase 7: Reconciliation Hardening

**Chaos Tests**:
- Kill shim mid-volume-creation → verify orphan cleanup
- Kill shim mid-container → verify container detection
- Network timeout simulation → verify retries
- Partial failure handling → verify transaction consistency

**Test Scenarios**:
```rust
#[test]
async fn test_crash_recovery_mid_volume_creation() { }

#[test]
async fn test_crash_recovery_mid_container_execution() { }

#[test]
async fn test_partial_failure_recovery() { }
```

**Manual Chaos Testing**:
```bash
cargo run & PID=$!
sleep 5
kill $PID
# Restart shim
cargo run &
# Verify orphans cleaned up
```

---

### Phase 8: Events + Metrics

**New Tests**:
- Event emission on state transitions
- Event query filtering
- Metrics calculation accuracy
- Metrics API format validation

**Test Scenarios**:
```rust
#[test]
async fn test_events_emitted_on_state_change() { }

#[test]
fn test_metrics_cpu_calculation() { }

#[test]
fn test_metrics_memory_estimation() { }
```

---

### Phase 9: Pricing

**New Tests**:
- Pricing API sync
- Cost calculation (hourly rates)
- Fallback pricing on API failure
- Cost tracking per job

**Test Scenarios**:
```rust
#[test]
async fn test_pricing_sync_from_hetzner() { }

#[test]
fn test_cost_calculation_accuracy() { }

#[test]
fn test_fallback_pricing_on_api_error() { }
```

---

### Phase 10: Resource Naming + Cleanup

**New Tests**:
- Resource name generation (convention validation)
- Cleanup script execution
- Orphan detection by age (7-day TTL)
- Batch deletion

**Test Scenarios**:
```rust
#[test]
fn test_resource_naming_format() { }

#[test]
async fn test_cleanup_old_resources() { }
```

---

### Phase 11: Production Testing

**Full E2E Tests**:
- Real osmdiffs workload
- 6+ hour job execution
- Cost accuracy verification
- Multiple scheduled runs
- Monitor for resource leaks

**Long-running Tests**:
```bash
# Deploy full configuration
terraform apply

# Monitor for 24 hours
for i in {1..24}; do
  echo "Hour $i: $(date)"
  sqlite3 db.sqlite "SELECT COUNT(*) FROM jobs WHERE status='Succeeded';"
  sleep 3600
done
```

---

## Test Fixtures and Helpers

Create `tests/fixtures.rs`:

```rust
pub fn create_test_secret(name: &str) -> serde_json::Value {
    serde_json::json!({
        "api_version": "v1",
        "kind": "Secret",
        "metadata": {"name": name},
        "data": {"key": "test-value"}
    })
}

pub fn create_test_cronjob(name: &str) -> serde_json::Value {
    serde_json::json!({
        "api_version": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": name},
        "spec": {
            "schedule": "0 2 * * *",
            "jobTemplate": {}
        }
    })
}

pub async fn setup_test_db() -> sqlx::SqlitePool {
    kube_shim::db::init_pool(":memory:")
        .await
        .expect("Failed to init test DB")
}
```

---

## Continuous Integration

Future phases will add:
- GitHub Actions for `cargo test` on every PR
- Code coverage reporting (tarpaulin)
- Benchmark tracking
- Linting (clippy)

**CI Script** (`.github/workflows/test.yml`):
```yaml
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test --all
      - run: cargo clippy -- -D warnings
```

---

## Test Coverage Goals

| Phase | Unit | Integration | E2E | Coverage |
|-------|------|-------------|-----|----------|
| 1 | ✅ 10+ | ✅ 8+ | ✅ | 80%+ |
| 2 | ✅ 5+ | ✅ 6+ | ✅ | 75%+ |
| 3 | ✅ 4+ | ✅ 4+ | ✅ | 70%+ |
| 4 | ✅ 5+ | ✅ 5+ | ✅ | 75%+ |
| 5 | ✅ 4+ | ✅ 4+ | ✅ | 70%+ |
| 6+ | ✅ 3+ | ✅ 3+ | ✅ | 70%+ |

---

## Running Tests in CI/CD

```bash
# Full test suite
cargo test --all --verbose

# With coverage
cargo tarpaulin --out Html --timeout 300

# Linting
cargo clippy -- -D warnings

# Format check
cargo fmt -- --check
```

---

## Debugging Failed Tests

```bash
# Run single test with output
cargo test test_name -- --nocapture

# Run with backtrace
RUST_BACKTRACE=1 cargo test

# Run with logging
RUST_LOG=debug cargo test -- --nocapture

# Run with database inspection
# (Pause test, inspect db.sqlite directly)
```

---

## Best Practices

1. **Test Organization**: Group related tests in modules
2. **Naming**: `test_<function>_<scenario>_<expected_result>`
3. **Setup/Teardown**: Use fixtures for common setup
4. **Isolation**: Each test should be independent
5. **Performance**: Keep unit tests fast (<100ms each)
6. **Documentation**: Comment complex test scenarios
7. **Mocking**: Mock external services (Hetzner, SSH)
8. **Assertions**: Use descriptive assertion messages

---

## Resources

- Rust Testing Guide: https://doc.rust-lang.org/book/ch11-00-testing.html
- Tokio Testing: https://tokio.rs/tokio/tutorial/testing
- SQLx Testing: https://github.com/launchbadge/sqlx#testing
