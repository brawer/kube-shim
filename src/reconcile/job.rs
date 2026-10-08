//! Job state machine. Phase 6 built it fully mocked; Phase 8 made volume
//! create/delete real; Phase 9 made the rest of the pipeline real too --
//! real worker VMs, real firewall rules, real volume attachment. Phase 10
//! closes the very last gap: `ContainerRunning` now genuinely detects
//! completion over SSH (`src/ssh.rs`), fetches the real exit code and full
//! container logs, and splits the `Succeeded`/`Failed` fork for real --
//! nothing in this pipeline is mocked or simulated anymore.
//!
//! **This replaces, not extends, Phase 9's own completion mechanism.**
//! Phase 9's `ContainerRunning` polled `get_server` and treated the worker
//! powering itself off (cloud-init's last action) as "done", specifically
//! because no SSH client existed yet to do better. Now that one does,
//! `bootstrap/cloud-init-template.sh` no longer powers the VM off at all --
//! it stays running (and billing) until the shim itself notices completion
//! over SSH and moves on to real cleanup (`VolumeDetaching`/`VMTerminating`
//! delete it shortly after, whether it's still running or not; UpCloud's
//! `delete_server` doesn't require a server to be stopped first). This is
//! a deliberate trade, not an oversight: a shim that crashes between the
//! container finishing and noticing it now leaves the worker running
//! (and billing) indefinitely, with nothing to stop it, where Phase 9's
//! mechanism would have self-terminated on a timer regardless. Closing
//! that gap for real means giving every state a timeout -- already
//! Phase 11's own stated job ("Reconciliation Hardening" lists volume/
//! VM/firewall timeouts as its own deliverable) -- rather than growing a
//! second, cruder timeout mechanism here just for this one transition.
//!
//! **Phase 11 closes that gap, plus two more real ones:**
//! - `advance_all` now enforces both `activeDeadlineSeconds` (the job's
//!   own declared worst-case bound, from `created_at`) and a uniform
//!   5-minute stuck-state timeout (from `last_transition_time`,
//!   reusing `RETRY_ESCALATION_THRESHOLD`) for every state short of
//!   `Succeeded`/`Failed` -- see `is_pre_completion_state`,
//!   `active_deadline_seconds`, and `force_failed`. Either bound firing
//!   forces the job straight to `"Failed"`, which rejoins the normal
//!   `VolumeDetaching`/`VMTerminating` cleanup path rather than needing
//!   its own cleanup logic.
//! - `handle_volume_pending`/`handle_vm_pending` now check for an
//!   already-existing, untracked volume/VM (by its exact expected
//!   title) before creating a new one -- closing the window where a
//!   crash between a real `create_volume`/`create_server` call
//!   succeeding and the corresponding DB row committing would otherwise
//!   launch a second, billable duplicate on the next retry. Orphan
//!   scanning (Phase 8/9) was already a backstop for this, eventually --
//!   this avoids creating the duplicate in the first place.
//! - Both `UpCloudProvider`'s HTTP client (`src/providers/upcloud/
//!   mod.rs`) and `src/ssh.rs`'s connections now have real timeouts --
//!   previously, a hung network call to either could block an entire
//!   reconciliation tick indefinitely, which no amount of state-level
//!   timeout logic here would ever notice or recover from.
//!
//! **Two real findings from Phase 9, still true, changing the pipeline's
//! shape from what was originally planned:**
//!
//! 1. **State order.** The original state list (see this project's own
//!    history) had `VolumeAttaching`/`VolumeAttached` *before* any VM
//!    states -- impossible for real: `attach_volume` needs a server UUID
//!    that doesn't exist until a VM is created. `STATE_SEQUENCE` below
//!    reorders VM creation before volume attachment.
//! 2. **Firewall timing.** UpCloud firewall rules are enforced the moment
//!    they're accepted (modulo ~1-2 min propagation, Phase 7), but cloud-
//!    init inside the VM starts running the instant it boots -- entirely
//!    outside this reconciliation loop's control. If firewall rules waited
//!    until the VM was confirmed `started`, the worker would sit fully
//!    open to the internet for its entire boot+`apt-get install podman`
//!    window. So `FirewallApplying` runs immediately after `VMPending`
//!    (server creation), not after `VMCreating` -- as early as physically
//!    possible, minimizing (not eliminating -- the propagation lag is
//!    real) that exposure window.
//!
//! **A third, smaller finding:** `VolumeDetaching`'s real handler now
//! calls `detach_volume` before `delete_volume` (there wasn't a VM to
//! detach from in Phase 8). A retry of this same state after a failed
//! `delete_volume` would call `detach_volume` again on an
//! already-detached volume -- treated as non-fatal (logged, not
//! retried-on) rather than blocking cleanup forever on a call that only
//! ever needs to succeed once; see `handle_volume_detaching`'s own docs.
//!
//! **Two kinds of "not yet" are tracked differently, deliberately.** A
//! one-shot action (`VolumePending`, `VMPending`, `FirewallApplying`,
//! `VolumeAttaching`, `VolumeDetaching`, `VMTerminating`) records a
//! failure via `record_failed_attempt` when its real call errors --
//! that's an actual problem worth escalating to `error` after 5 minutes
//! (see `RETRY_ESCALATION_THRESHOLD`). A polling state
//! (`FirewallVerified`, `VMCreating`, `ContainerRunning`) waiting for a
//! resource to reach a target state it hasn't reached *yet* is not a
//! failure -- normal boot time, normal propagation lag, normal container
//! runtime -- so it only logs at `debug` and never touches
//! `retry_count`/`last_error`. Phase 11 closes the gap this used to leave
//! open (a resource that never reaches its target state polling
//! silently forever) with a *separate* mechanism layered on top in
//! `advance_all` -- the uniform stuck-state timeout, driven by
//! `last_transition_time` regardless of whether this tick's own handler
//! even runs -- rather than teaching every polling handler its own
//! timeout logic individually.

use crate::providers::{
    CloudProvider, CreateServerRequest, CreateVolumeRequest, FirewallAction, FirewallDirection,
    FirewallFamily, FirewallRule, ProviderError,
};
use crate::{cloud_init, pricing, ssh, volumes, workload};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use uuid::Uuid;

/// The full pipeline a job run passes through -- see this module's own
/// top-level docs for why the order isn't a straight reading of
/// docs/IMPLEMENTATION_PLAN.md's original "Reconciliation Loop State
/// Machine" diagram (that diagram is updated to match this, not the other
/// way around). Minus `BudgetWait` (Phase 13 doesn't exist yet). The real
/// `Succeeded`/`Failed` fork (Phase 10) isn't in this array at all --
/// `"Failed"` is a lateral branch `next_state` special-cases, not a state
/// ever reached by walking forward from `"Created"`; see that function's
/// own docs.
const STATE_SEQUENCE: &[&str] = &[
    "Created",
    "VolumePending",
    "VolumeCreating",
    "VolumeCreated",
    "VMPending",
    "FirewallApplying",
    "FirewallVerified",
    "VMCreating",
    "VMRunning",
    "VolumeAttaching",
    "VolumeAttached",
    "ContainerRunning",
    "Succeeded",
    "VolumeDetaching",
    "VolumeDeleted",
    "VMTerminating",
    "Archived",
];

/// `Archived` is the only true terminal state in this pipeline -- even a
/// mocked "Succeeded" run still continues on to cleanup states, matching
/// how a real job run's lifecycle doesn't stop just because the workload
/// itself finished.
pub const TERMINAL_STATE: &str = "Archived";

/// How long a one-shot real action (see this module's top-level docs for
/// the one-shot/polling distinction) will keep silently retrying (still
/// every tick, just at `warn` level) before escalating to `error` --
/// observability only, not a give-up-and-clean-up mechanism. That's Phase
/// 11's job; see this module's own top-level docs on staying in scope.
const RETRY_ESCALATION_THRESHOLD: i64 = 5 * 60;

/// Boot disk size for every worker VM -- an OS-only disk, not the job's
/// own scratch space (that's the separately attached ephemeral volume).
/// Matches this project's own shim instance's boot disk convention
/// (`bootstrap/provision.sh`'s real UpCloud server).
const WORKER_BOOT_DISK_GB: u32 = 10;

/// Everything a job-advancing tick needs beyond the database: the real
/// `CloudProvider` to call, whether to actually call it, and the naming/
/// placement/credentials real calls need. Bundled into one struct (rather
/// than a long parameter list) since every real-work handler below needs
/// most of it.
#[derive(Clone)]
pub struct JobContext {
    pub provider: Arc<dyn CloudProvider>,
    /// From `config.toml`'s `[upcloud] dry_run`. Every state whose real
    /// action needs a previously-created resource ID (worker VM, volume)
    /// naturally no-ops in dry-run mode already, since dry-run never
    /// creates one to look up -- see this module's own top-level docs on
    /// why most states below don't need their own explicit dry-run
    /// branch. `VolumePending`/`VMPending` still log an explicit
    /// "DRY-RUN: would ..." line each, matching Phase 7's own convention,
    /// since those are the two places a dry-run trace actually has
    /// something distinct to say (size/tier, chosen plan).
    pub dry_run: bool,
    /// From `config.toml`'s `[shim] resource_prefix` (Phase 5) -- threaded
    /// through real volume/VM naming.
    pub resource_prefix: String,
    /// From `config.toml`'s `[upcloud] zone` (Phase 7).
    pub zone: String,
    /// From `config.toml`'s `[upcloud] worker_template_uuid` (Phase 9).
    pub worker_template_uuid: String,
    /// From `config.toml`'s `[upcloud] worker_ssh_public_keys` (Phase 9).
    pub worker_ssh_public_keys: Vec<String>,
    /// The shim's own current public IPv4, from `metadata::own_public_ipv4()`
    /// (Phase 9), computed once at startup and threaded through rather
    /// than re-queried every tick (it can't change while this process is
    /// running). `None` means "unknown" -- a worker VM created while this
    /// is `None` gets no inbound-SSH-allow rule at all, the safe direction
    /// to fail in, not a crash. See `src/metadata.rs`'s own docs for why
    /// this is queried dynamically rather than a static config value.
    pub own_public_ip: Option<String>,
    /// From `config.toml`'s `[upcloud] worker_ssh_private_key` (Phase 10).
    /// Empty means the shim can't SSH into any worker at all --
    /// `ContainerRunning` degrades to marking the job `Succeeded` with no
    /// real exit code or logs rather than polling forever (see
    /// `handle_container_running`).
    pub worker_ssh_private_key: String,
    /// Always `ssh::SSH_PORT` (22) in production -- a real worker VM's
    /// sshd never listens anywhere else. Exists as its own field only so
    /// tests can point `handle_container_running` at a local mock SSH
    /// server bound to an OS-assigned port, the same seam
    /// `UpCloudProvider::with_base_url` already gives UpCloud API tests
    /// (Phase 7).
    pub worker_ssh_port: u16,
    /// From `config.toml`'s `[shim] main_currency` (Phase 5, first real
    /// use in Phase 14a) -- every cost figure `handle_created`/
    /// `handle_container_running` compute is expressed in this currency,
    /// never the provider's own real billing currency directly (see
    /// `pricing::estimate_job_cost`'s own docs).
    pub main_currency: String,
}

/// The state one tick after `current`, or `None` if `current` is
/// `TERMINAL_STATE` or not a state this pipeline recognizes at all (e.g.
/// leftover data from a different schema version -- callers should leave
/// such a job alone and log a warning rather than guess).
///
/// `"Failed"` is deliberately *not* in `STATE_SEQUENCE` itself -- it's a
/// lateral branch `handle_container_running` (Phase 10) chooses instead
/// of `"Succeeded"` when the real exit code is nonzero, not a state ever
/// reached by walking the array forward from `"Created"`. It rejoins the
/// same cleanup path either way, so it just needs its own successor here.
pub fn next_state(current: &str) -> Option<&'static str> {
    if current == "Failed" {
        return Some("VolumeDetaching");
    }
    let index = STATE_SEQUENCE.iter().position(|state| *state == current)?;
    STATE_SEQUENCE.get(index + 1).copied()
}

/// True for every state strictly before `"Succeeded"` in `STATE_SEQUENCE`
/// -- the states Phase 11's `activeDeadlineSeconds`/stuck-state
/// enforcement (`advance_all`) actually applies to. `"Succeeded"`,
/// `"Failed"`, and everything in the cleanup tail after them (
/// `VolumeDetaching`/`VolumeDeleted`/`VMTerminating`) are excluded on
/// purpose: a job that's already finished or already cleaning up has
/// nothing left for a deadline to meaningfully cut short, and forcing it
/// to `"Failed"` again would either be a no-op (`next_state("Failed")`
/// already points at `VolumeDetaching`, same place a stuck cleanup state
/// already is) or actively wrong (overwriting a real `"Succeeded"`
/// outcome after the fact).
///
/// `pub(crate)`: also used by `reconcile::metrics_collector` to decide
/// which jobs are even worth an SSH attempt -- combined with
/// `worker_ssh_ip IS NOT NULL`, this naturally resolves to "from
/// `VMRunning` (the first state with a real SSH IP) through
/// `ContainerRunning`," without hardcoding that state list a second
/// time somewhere that could drift from `STATE_SEQUENCE`.
pub(crate) fn is_pre_completion_state(status: &str) -> bool {
    let succeeded_index = STATE_SEQUENCE
        .iter()
        .position(|s| *s == "Succeeded")
        .expect("Succeeded is always in STATE_SEQUENCE");
    match STATE_SEQUENCE.iter().position(|s| s == &status) {
        Some(index) => index < succeeded_index,
        None => false,
    }
}

/// `spec.activeDeadlineSeconds`, Kubernetes' own field name and location
/// (top-level on the pod template's enclosing spec -- see
/// `find_ephemeral_volume`'s own docs on this spec's shape). Phase 5's
/// admission check guarantees every real CronJob has this set, but a
/// missing/malformed value here just disables deadline enforcement for
/// that job rather than erroring -- the per-state stuck-timeout check
/// (`advance_all`) still applies regardless.
fn active_deadline_seconds(spec: &JsonValue) -> Option<i64> {
    spec.get("activeDeadlineSeconds")?.as_i64()
}

/// Force-fails a job that exceeded its `activeDeadlineSeconds` or got
/// stuck in one state for too long (Phase 11) -- sets `status = Failed`
/// directly (bypassing the normal per-state handlers entirely for this
/// tick) and records why in `last_error`. Deliberately does *not* try to
/// synchronously delete whatever volume/VM the job might have -- forcing
/// `"Failed"` is enough, since `next_state("Failed")` already rejoins the
/// exact same `VolumeDetaching` -> `VolumeDeleted` -> `VMTerminating`
/// cleanup path every other failure uses, which already tolerates
/// cleaning up a partial/nonexistent set of resources. A few extra ticks
/// (seconds, not minutes, given `FALLBACK_INTERVAL`) to actually finish
/// deleting things is an acceptable cost for not duplicating that cleanup
/// logic a second time here.
///
/// `pub(crate)`: also called directly by `api::job::delete_job` (Phase
/// 13) when a user deletes a still-running standalone Job -- the exact
/// same real-cleanup path, rather than a second teardown mechanism built
/// just for user-initiated delete.
pub(crate) async fn force_failed(
    pool: &SqlitePool,
    job_id: &str,
    namespace: &str,
    name: &str,
    reason: &str,
    now: i64,
) -> Result<()> {
    tracing::warn!("job {namespace}/{name}: forcing Failed: {reason}");
    sqlx::query(
        "UPDATE jobs SET status = 'Failed', last_error = ?, last_transition_time = ?, \
         updated_at = ?, version = version + 1 WHERE id = ?",
    )
    .bind(reason)
    .bind(now)
    .bind(now)
    .bind(job_id)
    .execute(pool)
    .await?;
    record_event(pool, job_id, "Failed", reason, "Warning").await?;
    Ok(())
}

/// Records one row in `events` (Phase 12) -- `kubectl describe pod` reads
/// these back via `api::events::list_events`, joined against `jobs` for
/// the job's own name/namespace (`events.job_id` is the job's internal
/// UUID, not its user-facing name). Deliberately one row per occurrence,
/// never de-duplicated/counted the way a real apiserver would collapse
/// repeated identical events -- this project's own transitions are each
/// already a distinct, one-time occurrence (a job never re-enters the
/// same state twice on its way from `Created` to `Archived`), so there's
/// nothing to collapse.
async fn record_event(
    pool: &SqlitePool,
    job_id: &str,
    reason: &str,
    message: &str,
    event_type: &str,
) -> Result<()> {
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO events (id, job_id, reason, message, timestamp, type) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(job_id)
    .bind(reason)
    .bind(message)
    .bind(now)
    .bind(event_type)
    .execute(pool)
    .await?;
    Ok(())
}

/// Advances every job not yet in `TERMINAL_STATE` by up to one state.
/// States with no real work (see `STATE_SEQUENCE`'s own docs) advance
/// unconditionally; every other state only advances once its real work
/// actually succeeds (a one-shot action) or its polled target is actually
/// reached (a polling state) -- otherwise the job stays put and tries
/// again next tick. `ContainerRunning` is the one state that can advance
/// to somewhere *other* than `next_state(status)` (`"Failed"` instead of
/// `"Succeeded"`), so every handler below reports the actual target
/// status to move to (`None` meaning "stay put"), not just whether to
/// advance. Returns how many jobs were advanced (for logging/testing).
pub async fn advance_all(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    let rows = sqlx::query(
        "SELECT id, name, namespace, status, spec, last_transition_time, created_at FROM jobs WHERE status != ?",
    )
    .bind(TERMINAL_STATE)
    .fetch_all(pool)
    .await?;

    let now = Utc::now().timestamp();
    let mut advanced = 0;
    for row in rows {
        let id: String = row.get(0);
        let name: String = row.get(1);
        let namespace: String = row.get(2);
        let status: String = row.get(3);
        let spec_str: String = row.get(4);
        let last_transition_time: Option<i64> = row.get(5);
        let created_at: i64 = row.get(6);

        let Some(next) = next_state(&status) else {
            tracing::warn!(
                "job {namespace}/{name} is in unrecognized state {status:?}, leaving it alone"
            );
            continue;
        };

        // Phase 11: a job still short of Succeeded/Failed gets force-
        // failed if either bound is exceeded -- checked before, and
        // instead of, this tick's normal per-state handling. See
        // `force_failed`'s own docs for why both checks exist and why
        // forcing "Failed" here (rather than deleting resources
        // synchronously) is enough: it rejoins the same real cleanup
        // path (`VolumeDetaching`/`VMTerminating`) every other failure
        // already uses.
        if is_pre_completion_state(&status) {
            let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
            if let Some(deadline) = active_deadline_seconds(&spec) {
                if now - created_at > deadline {
                    force_failed(
                        pool,
                        &id,
                        &namespace,
                        &name,
                        &format!(
                            "DeadlineExceeded: activeDeadlineSeconds ({deadline}s) exceeded \
                             while in state {status}"
                        ),
                        now,
                    )
                    .await?;
                    advanced += 1;
                    continue;
                }
            }

            let stuck_since = last_transition_time.unwrap_or(created_at);
            if now - stuck_since > RETRY_ESCALATION_THRESHOLD {
                force_failed(
                    pool,
                    &id,
                    &namespace,
                    &name,
                    &format!(
                        "StateTimeout: stuck in {status} for over {RETRY_ESCALATION_THRESHOLD}s"
                    ),
                    now,
                )
                .await?;
                advanced += 1;
                continue;
            }
        }

        let target_status: Option<&'static str> = match status.as_str() {
            "Created" => {
                handle_created(pool, ctx, &id, &namespace, &name, &spec_str).await?;
                Some(next)
            }
            "VolumePending" => handle_volume_pending(
                pool,
                ctx,
                &id,
                &namespace,
                &name,
                &spec_str,
                last_transition_time,
            )
            .await?
            .then_some(next),
            "VMPending" => handle_vm_pending(
                pool,
                ctx,
                &id,
                &namespace,
                &name,
                &spec_str,
                last_transition_time,
            )
            .await?
            .then_some(next),
            "FirewallApplying" => {
                handle_firewall_applying(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
                    .then_some(next)
            }
            "FirewallVerified" => handle_firewall_verified(pool, ctx, &id, &namespace, &name)
                .await?
                .then_some(next),
            "VMCreating" => handle_vm_creating(pool, ctx, &id, &namespace, &name)
                .await?
                .then_some(next),
            "VolumeAttaching" => {
                handle_volume_attaching(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
                    .then_some(next)
            }
            "ContainerRunning" => {
                handle_container_running(pool, ctx, &id, &namespace, &name, &spec_str, created_at)
                    .await?
            }
            "VolumeDetaching" => {
                handle_volume_detaching(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
                    .then_some(next)
            }
            "VMTerminating" => {
                handle_vm_terminating(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
                    .then_some(next)
            }
            _ => Some(next),
        };

        let Some(target_status) = target_status else {
            continue;
        };

        sqlx::query(
            r#"
            UPDATE jobs
            SET status = ?, last_transition_time = ?, updated_at = ?, version = version + 1
            WHERE id = ?
            "#,
        )
        .bind(target_status)
        .bind(now)
        .bind(now)
        .bind(&id)
        .execute(pool)
        .await?;

        tracing::info!("job {namespace}/{name}: {status} -> {target_status}");
        let event_type = if target_status == "Failed" {
            "Warning"
        } else {
            "Normal"
        };
        record_event(
            pool,
            &id,
            target_status,
            &format!("Transitioned from {status} to {target_status}"),
            event_type,
        )
        .await?;
        advanced += 1;
    }

    Ok(advanced)
}

/// Job `spec` (as stored by `reconcile::schedule::create_job_run`) is the
/// CronJob's `jobTemplate.spec` directly -- `template.spec.volumes[]`,
/// `template.spec.containers[]`, `activeDeadlineSeconds` all live at the
/// top level here, one JSON path segment shorter than in the CronJob's
/// own spec (see `src/admission.rs` for that longer form).
///
/// `pub(crate)`: also used by `pricing::cost_for_duration` (Phase 14a)
/// to know whether a job's cost includes a volume at all -- the exact
/// same extraction `handle_volume_pending` already does for real
/// provisioning, reused rather than duplicated.
pub(crate) fn find_ephemeral_volume(spec: &JsonValue) -> Option<&JsonValue> {
    spec.pointer("/template/spec/volumes")
        .and_then(JsonValue::as_array)?
        .iter()
        .find_map(|v| v.pointer("/ephemeral/volumeClaimTemplate/spec"))
}

/// `pub(crate)`: also used by `api::metrics` (Phase 12) to estimate
/// resource usage across every currently non-terminal job. Returns
/// `(cpu_millicores, memory_mb)` -- full precision, *not* rounded up to
/// whole cores/gigabytes the way `handle_vm_pending`'s own VM-sizing
/// needs (see `workload::parse_cpu_millicores`'s docs for why those are
/// two deliberately different concerns): a job that requested `"500m"`/
/// `"512Mi"` should be reported as having requested exactly that, not
/// `kubectl top`-overstated to a whole core/GB just because that's what
/// ended up provisioned underneath it.
pub(crate) fn extract_resource_requests(spec: &JsonValue) -> (Option<u32>, Option<u32>) {
    let requests = spec.pointer("/template/spec/containers/0/resources/requests");
    let cpu_millicores = requests
        .and_then(|r| r.get("cpu"))
        .and_then(JsonValue::as_str)
        .and_then(|q| workload::parse_cpu_millicores(q).ok());
    let memory_mb = requests
        .and_then(|r| r.get("memory"))
        .and_then(JsonValue::as_str)
        .and_then(|q| volumes::parse_storage_quantity_mb(q).ok());
    (cpu_millicores, memory_mb)
}

/// `Created`: computes the job's worst-case cost (Phase 14a), from its
/// own `activeDeadlineSeconds`, before any real provisioning starts --
/// stored for later display/budget use (Phase 14b), never blocking the
/// job itself: a pricing/exchange-rate cache that hasn't synced yet
/// (`pricing::estimate_job_cost` returning `None`) just leaves
/// `jobs.estimated_cost` unset, the same "stays absent rather than a
/// fake number" stance this project already takes for real metrics
/// (`api::metrics`'s own docs). Always advances regardless -- an
/// unestimated cost is not a reason to stall the job itself.
async fn handle_created(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    spec_str: &str,
) -> Result<()> {
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let Some(deadline) = active_deadline_seconds(&spec) else {
        return Ok(());
    };

    match pricing::estimate_job_cost(pool, &ctx.zone, &spec, deadline, &ctx.main_currency).await {
        Ok(Some(cost)) => {
            sqlx::query("UPDATE jobs SET estimated_cost = ? WHERE id = ?")
                .bind(cost)
                .bind(job_id)
                .execute(pool)
                .await?;
        }
        Ok(None) => {
            tracing::debug!(
                "job {namespace}/{name}: pricing not cached yet, estimated_cost left unset"
            );
        }
        Err(err) => {
            tracing::warn!("job {namespace}/{name}: estimated_cost calculation failed: {err:?}");
        }
    }
    Ok(())
}

/// `VolumePending`: create the job's ephemeral volume for real (or log
/// what would be created, in dry-run mode). Returns whether the job
/// should advance to `VolumeCreating` this tick.
#[allow(clippy::too_many_arguments)]
async fn handle_volume_pending(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    spec_str: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let Some(volume) = find_ephemeral_volume(&spec) else {
        tracing::debug!("job {namespace}/{name}: no ephemeral volume requested, nothing to create");
        return Ok(true);
    };

    let size_gb = volume
        .pointer("/resources/requests/storage")
        .and_then(JsonValue::as_str)
        .and_then(|q| volumes::parse_storage_quantity_gb(q).ok());
    let tier =
        volumes::StorageTier::parse(volume.get("storageClassName").and_then(JsonValue::as_str))
            .unwrap_or(volumes::StorageTier::Standard);

    let Some(size_gb) = size_gb else {
        tracing::warn!(
            "job {namespace}/{name}: no valid resources.requests.storage found, advancing \
             without creating a volume -- admission-time rejection for this isn't built yet"
        );
        return Ok(true);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would create volume for job {namespace}/{name}: {size_gb}GB, tier {tier:?}"
        );
        return Ok(true);
    }

    let title = format!("{}-vol-{name}", ctx.resource_prefix);

    // Phase 11: idempotency -- a crash between create_volume() succeeding
    // and insert_job_volume() committing below would otherwise create a
    // second, duplicate volume the next time this same retry runs. Check
    // for one with this job's exact expected title first and reuse it if
    // found, rather than relying solely on the orphan scanner (Phase 8)
    // to notice and clean up the duplicate later.
    if let Ok(existing) = ctx.provider.list_volumes(&ctx.zone).await {
        if let Some(found) = existing.into_iter().find(|v| v.title == title) {
            tracing::warn!(
                "job {namespace}/{name}: found existing untracked volume {} titled {title:?} \
                 (likely a crash before this was recorded last time) -- reusing it instead of \
                 creating a duplicate",
                found.id
            );
            insert_job_volume(pool, job_id, size_gb, tier, &found.id).await?;
            return Ok(true);
        }
    }

    let request = CreateVolumeRequest {
        size_gb,
        tier: tier.upcloud_tier().to_string(),
        title,
        zone: ctx.zone.clone(),
    };

    match ctx.provider.create_volume(request).await {
        Ok(volume) => {
            tracing::info!(
                "job {namespace}/{name}: created volume {} ({size_gb}GB, {tier:?})",
                volume.id
            );
            insert_job_volume(pool, job_id, size_gb, tier, &volume.id).await?;
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `VMPending`: create the job's worker VM for real (or log what would be
/// launched, in dry-run mode). Picks the smallest server plan that fits
/// the pod template's resource requests (defaulting to the smallest known
/// plan if none are set, matching that real Kubernetes treats them as
/// optional too), generates the worker's cloud-init script, and refuses
/// to create a server at all if no SSH key is configured (see
/// `JobContext::worker_ssh_public_keys`'s own docs on why that's not
/// recoverable after the fact). Returns whether the job should advance to
/// `FirewallApplying` this tick.
#[allow(clippy::too_many_arguments)]
async fn handle_vm_pending(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    spec_str: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let (cpu_millicores, memory_mb) = extract_resource_requests(&spec);
    // Rounded *up* here, deliberately: UpCloud has no fractional-core or
    // sub-GB-memory plans, so sizing the actual worker VM needs whole
    // units, even though the request itself (and everything reporting
    // on it, e.g. api::metrics) keeps full precision.
    let cpu_cores = cpu_millicores.unwrap_or(1000).div_ceil(1000);
    let memory_gb = memory_mb.unwrap_or(1024).div_ceil(1024);
    let plan = workload::smallest_fitting_server_plan(cpu_cores, memory_gb);

    let Some(plan) = plan else {
        let msg = format!(
            "no known server plan is big enough for {cpu_cores}/{memory_gb} CPU/GB requested"
        );
        record_failed_attempt(pool, job_id, namespace, name, &msg, last_transition_time).await?;
        return Ok(false);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would launch VM for job {namespace}/{name}: plan {} \
             ({cpu_cores} CPU / {memory_gb}GB requested)",
            plan.name
        );
        return Ok(true);
    }

    if ctx.worker_ssh_public_keys.is_empty() {
        record_failed_attempt(
            pool,
            job_id,
            namespace,
            name,
            "upcloud.worker_ssh_public_keys is empty -- refusing to create a worker VM with no \
             way to log in (UpCloud's cloud-init templates don't support the create_password \
             fallback)",
            last_transition_time,
        )
        .await?;
        return Ok(false);
    }

    let user_data = match cloud_init::generate(&spec) {
        Ok(script) => script,
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &format!("cloud-init generation failed: {err}"),
                last_transition_time,
            )
            .await?;
            return Ok(false);
        }
    };

    let title = format!("{}-worker-{name}", ctx.resource_prefix);

    // Phase 11: idempotency -- same reasoning as handle_volume_pending's
    // own check. A crash between create_server() succeeding and
    // set_worker_vm() committing below would otherwise launch a second,
    // duplicate (and billable) worker VM on this same retry.
    if let Ok(existing) = ctx.provider.list_servers(&ctx.zone).await {
        if let Some(found) = existing.into_iter().find(|s| s.title == title) {
            tracing::warn!(
                "job {namespace}/{name}: found existing untracked worker VM {} titled {title:?} \
                 (likely a crash before this was recorded last time) -- reusing it instead of \
                 creating a duplicate",
                found.id
            );
            set_worker_vm(pool, job_id, &found.id, &title).await?;
            return Ok(true);
        }
    }

    let request = CreateServerRequest {
        title: title.clone(),
        hostname: title.clone(),
        zone: ctx.zone.clone(),
        plan: plan.name.to_string(),
        template_uuid: ctx.worker_template_uuid.clone(),
        boot_disk_size_gb: WORKER_BOOT_DISK_GB,
        ssh_public_keys: ctx.worker_ssh_public_keys.clone(),
        user_data,
    };

    match ctx.provider.create_server(request).await {
        Ok(server) => {
            tracing::info!(
                "job {namespace}/{name}: created worker VM {} ({title}, plan {})",
                server.id,
                plan.name
            );
            set_worker_vm(pool, job_id, &server.id, &title).await?;
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `FirewallApplying`: request the worker's firewall rules -- all inbound
/// denied except SSH from the shim's own public IP (if known), all
/// outbound allowed (both address families) -- as early as the server
/// UUID exists, per this module's own top-level docs on why that's before
/// `VMCreating`, not after.
async fn handle_firewall_applying(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let Some(vm_id) = worker_vm_id(pool, job_id).await? else {
        // No VM was ever created (dry-run, or an unreachable state) --
        // nothing to apply rules to.
        return Ok(true);
    };
    if ctx.dry_run {
        return Ok(true);
    }

    let mut rules = vec![
        FirewallRule {
            direction: FirewallDirection::Out,
            action: FirewallAction::Accept,
            family: FirewallFamily::Ipv4,
            protocol: None,
            source_address: None,
            destination_port_start: None,
            destination_port_end: None,
        },
        FirewallRule {
            direction: FirewallDirection::Out,
            action: FirewallAction::Accept,
            family: FirewallFamily::Ipv6,
            protocol: None,
            source_address: None,
            destination_port_start: None,
            destination_port_end: None,
        },
    ];

    match &ctx.own_public_ip {
        Some(own_ip) => rules.push(FirewallRule {
            direction: FirewallDirection::In,
            action: FirewallAction::Accept,
            family: FirewallFamily::Ipv4,
            protocol: Some("tcp".to_string()),
            source_address: Some(own_ip.clone()),
            destination_port_start: Some("22".to_string()),
            destination_port_end: Some("22".to_string()),
        }),
        None => tracing::warn!(
            "job {namespace}/{name}: shim's own public IP is unknown, worker VM {vm_id} will \
             have no inbound SSH access at all (see src/metadata.rs)"
        ),
    }

    match ctx.provider.create_firewall_rules(&vm_id, &rules).await {
        Ok(()) => {
            tracing::info!("job {namespace}/{name}: applied firewall rules to {vm_id}");
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `FirewallVerified`: poll until the rules `FirewallApplying` requested
/// are actually visible via `list_firewall_rules` -- UpCloud's own rule
/// application lags the accept-response by roughly 1-2 minutes (Phase 7).
/// A polling state (see this module's top-level docs): not-yet-applied is
/// logged at `debug` and retried, never treated as a failed attempt.
async fn handle_firewall_verified(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
) -> Result<bool> {
    let Some(vm_id) = worker_vm_id(pool, job_id).await? else {
        return Ok(true);
    };
    if ctx.dry_run {
        return Ok(true);
    }

    match ctx.provider.list_firewall_rules(&vm_id).await {
        Ok(rules) => {
            let outbound_allowed = rules.iter().any(|r| {
                r.direction == FirewallDirection::Out && r.action == FirewallAction::Accept
            });
            if outbound_allowed {
                tracing::info!(
                    "job {namespace}/{name}: firewall rules confirmed applied on {vm_id}"
                );
                Ok(true)
            } else {
                tracing::debug!(
                    "job {namespace}/{name}: firewall rules not applied yet on {vm_id}, waiting"
                );
                Ok(false)
            }
        }
        Err(err) => {
            tracing::warn!(
                "job {namespace}/{name}: failed to check firewall rules on {vm_id}, will retry: {err}"
            );
            Ok(false)
        }
    }
}

/// `VMCreating`: poll until the worker VM UpCloud reports it as `started`
/// (server creation is asynchronous -- Phase 7), then record its public
/// IPv4 (`jobs.worker_ssh_ip`, needed by Phase 10's log streaming). A
/// polling state: still booting is logged at `debug`, never a failed
/// attempt.
async fn handle_vm_creating(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
) -> Result<bool> {
    let Some(vm_id) = worker_vm_id(pool, job_id).await? else {
        return Ok(true);
    };
    if ctx.dry_run {
        return Ok(true);
    }

    match ctx.provider.get_server(&vm_id).await {
        Ok(server) if server.is_started() => {
            if let Some(ip) = &server.public_ipv4 {
                set_worker_ssh_ip(pool, job_id, ip).await?;
            }
            tracing::info!(
                "job {namespace}/{name}: worker VM {vm_id} is running (ip {:?})",
                server.public_ipv4
            );
            Ok(true)
        }
        Ok(server) => {
            tracing::debug!(
                "job {namespace}/{name}: worker VM {vm_id} still {}, waiting",
                server.state
            );
            Ok(false)
        }
        Err(err) => {
            tracing::warn!(
                "job {namespace}/{name}: failed to poll worker VM {vm_id}, will retry: {err}"
            );
            Ok(false)
        }
    }
}

/// `VolumeAttaching`: attach the job's ephemeral volume (if it requested
/// one) to its now-running worker VM. Returns whether the job should
/// advance to `VolumeAttached` this tick.
async fn handle_volume_attaching(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let Some(volume_id) = job_volume_id(pool, job_id).await? else {
        tracing::debug!("job {namespace}/{name}: no volume to attach");
        return Ok(true);
    };
    if ctx.dry_run {
        tracing::info!("DRY-RUN: would attach volume {volume_id} for job {namespace}/{name}");
        return Ok(true);
    }
    let Some(vm_id) = worker_vm_id(pool, job_id).await? else {
        tracing::warn!(
            "job {namespace}/{name}: no worker_vm_id recorded, cannot attach volume {volume_id}"
        );
        return Ok(true);
    };

    match ctx.provider.attach_volume(&vm_id, &volume_id).await {
        Ok(()) => {
            tracing::info!("job {namespace}/{name}: attached volume {volume_id} to {vm_id}");
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `ContainerRunning`: the container itself was already started
/// autonomously by cloud-init, not by this handler -- there's nothing for
/// the shim to *do* here, only to notice when it's done. SSHes in and
/// checks for `/tmp/exit-code` (written by `bootstrap/cloud-init-
/// template.sh` as its very last action, once `podman wait` returns) --
/// its absence or emptiness means the container is still running, a
/// polling case logged at `debug`, never a failed attempt. Once found,
/// fetches the full container logs too (`podman logs {id}`) and caches
/// both in `jobs.exit_code`/`.cached_logs` before choosing the real next
/// state: `"Succeeded"` for exit code 0, `"Failed"` for anything else --
/// see `next_state`'s own docs for how `"Failed"` rejoins the normal
/// cleanup path from there.
async fn handle_container_running(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    spec_str: &str,
    created_at: i64,
) -> Result<Option<&'static str>> {
    if ctx.dry_run {
        return Ok(Some("Succeeded"));
    }
    let Some(ip) = worker_ssh_ip(pool, job_id).await? else {
        // Shouldn't happen (VMCreating always records this before
        // advancing) -- treat defensively as "nothing to check".
        return Ok(Some("Succeeded"));
    };
    if ctx.worker_ssh_private_key.is_empty() {
        tracing::warn!(
            "job {namespace}/{name}: upcloud.worker_ssh_private_key is empty, cannot fetch the \
             real exit code/logs from {ip} -- marking Succeeded without them"
        );
        return Ok(Some("Succeeded"));
    }

    let exit_code_output = match ssh::exec_once(
        &ip,
        ctx.worker_ssh_port,
        &ctx.worker_ssh_private_key,
        "cat /tmp/exit-code",
    )
    .await
    {
        Ok(output) => output,
        Err(err) => {
            tracing::debug!(
                "job {namespace}/{name}: SSH to worker {ip} not ready yet, will retry: {err}"
            );
            return Ok(None);
        }
    };
    let Ok(exit_code) = String::from_utf8_lossy(&exit_code_output.stdout)
        .trim()
        .parse::<i64>()
    else {
        // The file doesn't exist yet (or is still empty) -- the script
        // hasn't finished, not a failure.
        tracing::debug!("job {namespace}/{name}: container still running on {ip}");
        return Ok(None);
    };

    let container_id = ssh::exec_once(
        &ip,
        ctx.worker_ssh_port,
        &ctx.worker_ssh_private_key,
        "cat /tmp/container-id.txt",
    )
    .await
    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    .unwrap_or_default();

    // Sanity-checked before ever being interpolated into a shell command
    // below -- container_id is normally our own cloud-init script's own
    // output, but a defensive check costs nothing and means a garbled/
    // unexpected read can never become a shell-injection vector.
    let logs = if !container_id.is_empty() && container_id.chars().all(|c| c.is_ascii_hexdigit()) {
        ssh::exec_once(
            &ip,
            ctx.worker_ssh_port,
            &ctx.worker_ssh_private_key,
            &format!("podman logs {container_id}"),
        )
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default()
    } else {
        tracing::warn!(
            "job {namespace}/{name}: no valid container ID read from {ip}, logs unavailable"
        );
        String::new()
    };

    // Real cost (Phase 14a), from the job's real elapsed wall-clock
    // duration -- not the conservative activeDeadlineSeconds estimate
    // `handle_created` already stored. Best-effort, same as that
    // estimate: a pricing/exchange-rate cache gap just leaves
    // `jobs.actual_cost` unset rather than blocking completion.
    let duration_seconds = Utc::now().timestamp() - created_at;
    let spec: JsonValue = serde_json::from_str(spec_str).unwrap_or(JsonValue::Null);
    let actual_cost = match pricing::calculate_actual_cost(
        pool,
        &ctx.zone,
        &spec,
        duration_seconds,
        &ctx.main_currency,
    )
    .await
    {
        Ok(cost) => cost,
        Err(err) => {
            tracing::warn!("job {namespace}/{name}: actual_cost calculation failed: {err:?}");
            None
        }
    };

    record_completion(pool, job_id, exit_code, &logs, actual_cost).await?;
    tracing::info!("job {namespace}/{name}: container finished on {ip}, exit code {exit_code}");
    Ok(Some(if exit_code == 0 {
        "Succeeded"
    } else {
        "Failed"
    }))
}

/// `VolumeDetaching`: detach (if a worker VM exists) then delete the
/// job's real volume, if one was ever created. A detach failure is logged
/// but not fatal -- see this module's own top-level docs on why a retry
/// after a failed `delete_volume` must not get stuck re-detaching an
/// already-detached volume forever. Returns whether the job should
/// advance to `VolumeDeleted` this tick.
async fn handle_volume_detaching(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let Some(provider_volume_id) = job_volume_id(pool, job_id).await? else {
        tracing::debug!(
            "job {namespace}/{name}: no volume was ever created for this run, nothing to delete"
        );
        return Ok(true);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would detach and delete volume {provider_volume_id} for job {namespace}/{name}"
        );
        return Ok(true);
    }

    if let Some(vm_id) = worker_vm_id(pool, job_id).await? {
        if let Err(err) = ctx
            .provider
            .detach_volume(&vm_id, &provider_volume_id)
            .await
        {
            tracing::warn!(
                "job {namespace}/{name}: detach_volume for {provider_volume_id} from {vm_id} \
                 failed (continuing to delete anyway -- a retry would hit this again on an \
                 already-detached volume): {err}"
            );
        }
    }

    match ctx.provider.delete_volume(&provider_volume_id).await {
        Ok(()) => {
            tracing::info!("job {namespace}/{name}: deleted volume {provider_volume_id}");
            delete_job_volume(pool, job_id).await?;
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

/// `VMTerminating`: delete the job's worker VM, if one was ever created.
/// Runs after `VolumeDetaching`/`VolumeDeleted` specifically because
/// detaching a volume needs the server to still exist. Returns whether
/// the job should advance to `Archived` this tick.
async fn handle_vm_terminating(
    pool: &SqlitePool,
    ctx: &JobContext,
    job_id: &str,
    namespace: &str,
    name: &str,
    last_transition_time: Option<i64>,
) -> Result<bool> {
    let Some(vm_id) = worker_vm_id(pool, job_id).await? else {
        tracing::debug!(
            "job {namespace}/{name}: no worker VM was ever created, nothing to terminate"
        );
        return Ok(true);
    };
    if ctx.dry_run {
        tracing::info!("DRY-RUN: would delete worker VM {vm_id} for job {namespace}/{name}");
        return Ok(true);
    }

    // UpCloud rejects delete_server on a server that isn't already
    // "stopped" (409 SERVER_STATE_ILLEGAL) -- real finding from Phase 11's
    // own live verification, not assumed. Since Phase 10 leaves the worker
    // VM genuinely running right up until this state, this check isn't
    // optional: without it, every real job's cleanup would hit this same
    // error and retry forever (VMTerminating has no stuck-state timeout of
    // its own, by design -- see is_pre_completion_state).
    let server = match ctx.provider.get_server(&vm_id).await {
        Ok(server) => Some(server),
        Err(ProviderError::NotFound(_)) => None,
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            return Ok(false);
        }
    };

    if let Some(server) = &server {
        if !server.is_stopped() {
            if server.is_started() {
                tracing::info!(
                    "job {namespace}/{name}: stopping worker VM {vm_id} before deleting it"
                );
                if let Err(err) = ctx.provider.stop_server(&vm_id).await {
                    record_failed_attempt(
                        pool,
                        job_id,
                        namespace,
                        name,
                        &err.to_string(),
                        last_transition_time,
                    )
                    .await?;
                    return Ok(false);
                }
            } else {
                tracing::debug!(
                    "job {namespace}/{name}: waiting for worker VM {vm_id} to stop \
                     (state: {}) before deleting it",
                    server.state
                );
            }
            return Ok(false);
        }
    }

    match ctx.provider.delete_server(&vm_id).await {
        Ok(()) => {
            tracing::info!("job {namespace}/{name}: deleted worker VM {vm_id}");
            Ok(true)
        }
        Err(err) => {
            record_failed_attempt(
                pool,
                job_id,
                namespace,
                name,
                &err.to_string(),
                last_transition_time,
            )
            .await?;
            Ok(false)
        }
    }
}

async fn insert_job_volume(
    pool: &SqlitePool,
    job_id: &str,
    size_gb: u32,
    tier: volumes::StorageTier,
    provider_volume_id: &str,
) -> Result<()> {
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    sqlx::query(
        r#"
        INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, mount_point, created_at)
        VALUES (?, ?, ?, ?, ?, NULL, ?)
        "#,
    )
    .bind(id)
    .bind(job_id)
    .bind(size_gb)
    .bind(tier.class_name())
    .bind(provider_volume_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

async fn job_volume_id(pool: &SqlitePool, job_id: &str) -> Result<Option<String>> {
    let id: Option<String> =
        sqlx::query_scalar("SELECT provider_volume_id FROM job_volumes WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(pool)
            .await?
            .flatten();
    Ok(id)
}

async fn delete_job_volume(pool: &SqlitePool, job_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM job_volumes WHERE job_id = ?")
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn set_worker_vm(pool: &SqlitePool, job_id: &str, vm_id: &str, vm_name: &str) -> Result<()> {
    sqlx::query("UPDATE jobs SET worker_vm_id = ?, worker_vm_name = ? WHERE id = ?")
        .bind(vm_id)
        .bind(vm_name)
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn worker_vm_id(pool: &SqlitePool, job_id: &str) -> Result<Option<String>> {
    let id: Option<String> = sqlx::query_scalar("SELECT worker_vm_id FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    Ok(id)
}

async fn set_worker_ssh_ip(pool: &SqlitePool, job_id: &str, ip: &str) -> Result<()> {
    sqlx::query("UPDATE jobs SET worker_ssh_ip = ? WHERE id = ?")
        .bind(ip)
        .bind(job_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn worker_ssh_ip(pool: &SqlitePool, job_id: &str) -> Result<Option<String>> {
    let ip: Option<String> = sqlx::query_scalar("SELECT worker_ssh_ip FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    Ok(ip)
}

/// Records a job's real completion (Phase 10): its exit code and the
/// worker's full container logs, fetched over SSH by
/// `handle_container_running` while the worker VM is still up -- the only
/// time they're ever fetchable, since `VMTerminating` deletes the VM a
/// few states later regardless of whether cleanup succeeds.
/// `jobs.cached_logs` is what `api::logs`'s non-follow/fallback path
/// serves once the worker is gone.
async fn record_completion(
    pool: &SqlitePool,
    job_id: &str,
    exit_code: i64,
    logs: &str,
    actual_cost: Option<f64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET exit_code = ?, cached_logs = ?, actual_cost = ?, completed_at = ? \
         WHERE id = ?",
    )
    .bind(exit_code)
    .bind(logs)
    .bind(actual_cost)
    .bind(Utc::now().timestamp())
    .bind(job_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records a failed real-call attempt (`retry_count`/`last_error`,
/// both existing columns front-loaded in Phase 1's original schema) and
/// logs it -- at `warn` normally, escalating to `error` once it's been
/// failing for more than `RETRY_ESCALATION_THRESHOLD`. The job always
/// stays in its current state and retries next tick either way: deciding
/// when to actually give up and clean up is Phase 11's job, not this
/// one's -- this is purely about making a stuck job impossible to miss
/// in the logs. Only ever called for a one-shot action's real failure --
/// see this module's own top-level docs on why polling states don't use
/// this for "not ready yet".
async fn record_failed_attempt(
    pool: &SqlitePool,
    job_id: &str,
    namespace: &str,
    name: &str,
    error: &str,
    last_transition_time: Option<i64>,
) -> Result<()> {
    let now = Utc::now().timestamp();
    sqlx::query("UPDATE jobs SET retry_count = retry_count + 1, last_error = ?, updated_at = ? WHERE id = ?")
        .bind(error)
        .bind(now)
        .bind(job_id)
        .execute(pool)
        .await?;

    let retry_count: i64 = sqlx::query_scalar("SELECT retry_count FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(pool)
        .await?;
    let elapsed = now - last_transition_time.unwrap_or(now);

    if elapsed > RETRY_ESCALATION_THRESHOLD {
        tracing::error!(
            "job {namespace}/{name}: still failing after {elapsed}s (retry #{retry_count}): \
             {error} -- no automatic give-up/cleanup exists yet (Phase 11)"
        );
    } else {
        tracing::warn!(
            "job {namespace}/{name}: attempt failed (retry #{retry_count}), will retry: {error}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::upcloud::UpCloudProvider;
    use std::collections::HashMap;

    fn mock_ctx(dry_run: bool) -> JobContext {
        JobContext {
            provider: Arc::new(UpCloudProvider::new("unused-in-dry-run")),
            dry_run,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec!["ssh-ed25519 AAAA test".to_string()],
            own_public_ip: Some("203.0.113.5".to_string()),
            worker_ssh_private_key: crate::ssh::tests::throwaway_private_key_pem(),
            worker_ssh_port: ssh::SSH_PORT,
            main_currency: "EUR".to_string(),
        }
    }

    #[test]
    fn test_next_state_walks_the_full_sequence() {
        let mut state = "Created";
        let mut steps = 0;
        while let Some(next) = next_state(state) {
            state = next;
            steps += 1;
            assert!(steps < 100, "state sequence should terminate");
        }
        assert_eq!(state, TERMINAL_STATE);
        assert_eq!(steps, STATE_SEQUENCE.len() - 1);
    }

    #[test]
    fn test_next_state_terminal_has_no_successor() {
        assert_eq!(next_state(TERMINAL_STATE), None);
    }

    #[test]
    fn test_next_state_unknown_state_returns_none() {
        assert_eq!(next_state("SomeStateFromAFutureSchemaVersion"), None);
    }

    #[test]
    fn test_vm_states_come_before_volume_attach_states() {
        // The real ordering finding this module's top-level docs describe
        // -- attach_volume needs a server UUID, so VM creation must
        // precede it.
        let vm_pending = STATE_SEQUENCE
            .iter()
            .position(|s| *s == "VMPending")
            .unwrap();
        let attaching = STATE_SEQUENCE
            .iter()
            .position(|s| *s == "VolumeAttaching")
            .unwrap();
        assert!(vm_pending < attaching);
    }

    #[tokio::test]
    async fn test_advance_all_moves_each_job_one_step() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', ?, ?, 1)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumePending");
    }

    #[tokio::test]
    async fn test_advance_all_ignores_terminal_jobs() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Archived', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 0);
    }

    #[tokio::test]
    async fn test_advance_all_increments_version() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', ?, ?, 1)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &mock_ctx(true)).await.unwrap();

        let version: i64 = sqlx::query_scalar("SELECT version FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    fn ephemeral_volume_spec() -> String {
        serde_json::json!({
            "template": {"spec": {
                "volumes": [{"ephemeral": {"volumeClaimTemplate": {"spec": {
                    "resources": {"requests": {"storage": "1Gi"}},
                    "storageClassName": "kube-shim-fast"
                }}}}],
                "containers": [{"image": "busybox:latest"}]
            }}
        })
        .to_string()
    }

    #[tokio::test]
    async fn test_volume_pending_dry_run_advances_without_provider_call() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeCreating");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "dry-run must never write a job_volumes row");
    }

    #[tokio::test]
    async fn test_volume_pending_with_no_ephemeral_volume_advances_without_creating_one() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{\"template\":{\"spec\":{}}}', 'VolumePending', ?, ?, 1)",
        )
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(false)).await.unwrap();
        assert_eq!(advanced, 1);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_volume_pending_real_success_creates_volume_and_advances() {
        let app = axum::Router::new().route(
            "/1.3/storage",
            axum::routing::post(|| async {
                axum::Json(serde_json::json!({
                    "storage": {"uuid": "vol-real-1", "size": 1, "tier": "maxiops", "title": "t", "zone": "de-fra1"}
                }))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeCreating");

        let (provider_volume_id, storage_class_name): (String, String) = sqlx::query_as(
            "SELECT provider_volume_id, storage_class_name FROM job_volumes WHERE job_id = 'j1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(provider_volume_id, "vol-real-1");
        assert_eq!(storage_class_name, "kube-shim-fast");
    }

    #[tokio::test]
    async fn test_volume_pending_real_failure_retries_and_records_error() {
        let app = axum::Router::new().route(
            "/1.3/storage",
            axum::routing::post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0, "a failed real call must not advance the job");

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumePending", "must stay put to retry next tick");

        let (retry_count, last_error): (i64, Option<String>) =
            sqlx::query_as("SELECT retry_count, last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(retry_count, 1);
        assert!(!last_error.unwrap().is_empty());

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "a failed create must never leave a job_volumes row behind"
        );
    }

    #[tokio::test]
    async fn test_vm_pending_dry_run_advances_without_provider_call() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, worker_vm_id): (String, Option<String>) =
            sqlx::query_as("SELECT status, worker_vm_id FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FirewallApplying");
        assert!(
            worker_vm_id.is_none(),
            "dry-run must never create a real VM"
        );
    }

    #[tokio::test]
    async fn test_vm_pending_rounds_fractional_request_up_to_a_whole_plan() {
        // UpCloud has no fractional-core/sub-GB-memory plans -- a job
        // that asked for "500m"/"512Mi" must still get sized onto a
        // real, whole-unit plan (the smallest one that fits), even
        // though extract_resource_requests itself now preserves that
        // fractional precision for api::metrics's own purposes.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        let spec = serde_json::json!({
            "template": {"spec": {
                "containers": [{"image": "x", "resources": {"requests": {"cpu": "500m", "memory": "512Mi"}}}]
            }}
        })
        .to_string();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(spec)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(
            advanced, 1,
            "a fractional request must still find a fitting plan, not error out"
        );

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "FirewallApplying");
    }

    #[tokio::test]
    async fn test_vm_pending_refuses_with_no_ssh_keys_configured() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_public_keys = vec![];

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0);

        let (status, last_error): (String, Option<String>) =
            sqlx::query_as("SELECT status, last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "VMPending");
        assert!(last_error.unwrap().contains("worker_ssh_public_keys"));
    }

    #[tokio::test]
    async fn test_vm_pending_real_success_creates_server_and_advances() {
        let app = axum::Router::new().route(
            "/1.3/server",
            axum::routing::post(|| async {
                axum::Json(serde_json::json!({
                    "server": {"uuid": "srv-real-1", "title": "t", "state": "maintenance"}
                }))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, worker_vm_id, worker_vm_name): (String, Option<String>, Option<String>) =
            sqlx::query_as("SELECT status, worker_vm_id, worker_vm_name FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FirewallApplying");
        assert_eq!(worker_vm_id.as_deref(), Some("srv-real-1"));
        assert_eq!(
            worker_vm_name.as_deref(),
            Some("kube-shim-test-worker-job-one")
        );
    }

    #[tokio::test]
    async fn test_vm_pending_no_fitting_plan_records_error() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let spec = serde_json::json!({
            "template": {"spec": {
                "containers": [{"image": "x", "resources": {"requests": {"cpu": "64", "memory": "256Gi"}}}]
            }}
        })
        .to_string();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(spec)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(false)).await.unwrap();
        assert_eq!(advanced, 0);

        let last_error: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(last_error.unwrap().contains("no known server plan"));
    }

    async fn insert_job_with_vm(pool: &SqlitePool, status: &str, vm_id: &str) {
        // created_at/last_transition_time must be realistic (not the
        // literal epoch) -- Phase 11's stuck-timeout/deadline enforcement
        // in advance_all() treats a row that looks decades old exactly
        // like a real, genuinely stuck job and force-fails it.
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', '{}', ?, ?, ?, ?, ?, 1)",
        )
        .bind(status)
        .bind(vm_id)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_firewall_applying_includes_ssh_rule_from_own_ip() {
        // create_firewall_rules POSTs one rule per call (outbound x2, then
        // inbound) -- collect every body rather than asserting inline in
        // the handler, since only the *last* call carries the IP.
        let bodies: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies_clone = bodies.clone();
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::post(move |body: String| {
                let bodies = bodies_clone.clone();
                async move {
                    bodies.lock().unwrap().push(body);
                    axum::Json(serde_json::json!({"firewall_rule": {}}))
                }
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "FirewallApplying", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "FirewallVerified");

        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3, "2 outbound rules + 1 inbound SSH rule");
        assert!(bodies
            .iter()
            .any(|b| b.contains("203.0.113.5") && b.contains("\"destination_port_start\":\"22\"")));
    }

    #[tokio::test]
    async fn test_firewall_applying_with_unknown_own_ip_skips_ssh_rule() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::post(|body: String| async move {
                assert!(!body.contains("\"direction\":\"in\""));
                axum::Json(serde_json::json!({"firewall_rule": {}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);
        ctx.own_public_ip = None;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "FirewallApplying", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
    }

    #[tokio::test]
    async fn test_firewall_verified_waits_until_rules_present() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"firewall_rules": {"firewall_rule": []}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "FirewallVerified", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(
            advanced, 0,
            "must not advance until rules are actually visible"
        );

        let (retry_count, status): (i64, String) =
            sqlx::query_as("SELECT retry_count, status FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FirewallVerified");
        assert_eq!(
            retry_count, 0,
            "not-ready-yet must not count as a failed attempt"
        );
    }

    #[tokio::test]
    async fn test_firewall_verified_advances_once_outbound_rule_present() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"firewall_rules": {"firewall_rule": [
                    {"direction": "out", "action": "accept", "family": "IPv4"}
                ]}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "FirewallVerified", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
    }

    #[tokio::test]
    async fn test_vm_creating_waits_while_maintenance_then_advances_once_started() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({
                    "server": {"uuid": "srv1", "title": "t", "state": "started",
                               "ip_addresses": {"ip_address": [
                                   {"access": "public", "family": "IPv4", "address": "1.2.3.4"}
                               ]}}
                }))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMCreating", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, worker_ssh_ip): (String, Option<String>) =
            sqlx::query_as("SELECT status, worker_ssh_ip FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "VMRunning");
        assert_eq!(worker_ssh_ip.as_deref(), Some("1.2.3.4"));
    }

    #[tokio::test]
    async fn test_vm_creating_still_maintenance_does_not_advance() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "maintenance"}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMCreating", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0);
    }

    #[tokio::test]
    async fn test_volume_attaching_calls_provider_when_volume_tracked() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/storage/attach",
            axum::routing::post(|| async { axum::Json(serde_json::json!({"server": {}})) }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VolumeAttaching", "srv1").await;
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol1', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
    }

    #[tokio::test]
    async fn test_volume_attaching_with_no_volume_advances_without_calling_provider() {
        // No route registered -- would fail loudly if attach_volume were
        // called for a job that never requested a volume.
        let app = axum::Router::new();
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VolumeAttaching", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
    }

    async fn insert_job_with_ssh_ip(pool: &SqlitePool, status: &str, ip: &str) {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, worker_ssh_ip, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', '{}', ?, 'srv1', ?, ?, ?, ?, 1)",
        )
        .bind(status)
        .bind(ip)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_container_running_waits_while_no_exit_code_file_yet() {
        // No "cat /tmp/exit-code" entry -- the mock server's default
        // (empty stdout, exit 1) simulates the file not existing yet.
        let (host, port) = crate::ssh::tests::mock_ssh_server(HashMap::new()).await;
        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_port = port;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_ssh_ip(&pool, "ContainerRunning", &host).await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0, "must not advance until /tmp/exit-code exists");

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "ContainerRunning");
    }

    #[tokio::test]
    async fn test_container_running_zero_exit_code_advances_to_succeeded() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/exit-code", ("0", 0));
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert("podman logs abc123", ("hello from the container\n", 0));
        let (host, port) = crate::ssh::tests::mock_ssh_server(responses).await;
        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_port = port;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_ssh_ip(&pool, "ContainerRunning", &host).await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, exit_code, cached_logs): (String, Option<i64>, Option<String>) =
            sqlx::query_as("SELECT status, exit_code, cached_logs FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Succeeded");
        assert_eq!(exit_code, Some(0));
        assert_eq!(cached_logs.as_deref(), Some("hello from the container\n"));
    }

    async fn insert_provider_price(pool: &SqlitePool, price_key: &str, amount: f64, price: f64) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', ?, ?, ?, 'EUR', 0)",
        )
        .bind(price_key)
        .bind(amount)
        .bind(price)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_created_computes_estimated_cost_when_pricing_is_cached() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_provider_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'Created', ?, ?, 1)",
        )
        .bind(r#"{"activeDeadlineSeconds": 3600}"#)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &mock_ctx(false)).await.unwrap();

        let estimated_cost: Option<f64> =
            sqlx::query_scalar("SELECT estimated_cost FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // 1 hour at the default 1-CPU/1GB plan's own cents/hour price.
        assert!((estimated_cost.unwrap() - 0.004464).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_created_leaves_estimated_cost_unset_when_pricing_not_cached() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // No provider_pricing row at all.
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'Created', ?, ?, 1)",
        )
        .bind(r#"{"activeDeadlineSeconds": 3600}"#)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &mock_ctx(false)).await.unwrap();

        let (status, estimated_cost): (String, Option<f64>) =
            sqlx::query_as("SELECT status, estimated_cost FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // Advances regardless -- an unestimated cost never blocks the job.
        assert_eq!(status, "VolumePending");
        assert_eq!(estimated_cost, None);
    }

    #[tokio::test]
    async fn test_container_running_records_actual_cost_on_completion() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/exit-code", ("0", 0));
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert("podman logs abc123", ("hello\n", 0));
        let (host, port) = crate::ssh::tests::mock_ssh_server(responses).await;
        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_port = port;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_provider_price(&pool, "server_plan_DEV-1xCPU-1GB-10GB", 1.0, 0.4464).await;
        let now = Utc::now().timestamp();
        let one_hour_ago = now - 3600;
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, worker_ssh_ip, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'ContainerRunning', 'srv1', ?, ?, ?, ?, 1)",
        )
        .bind(&host)
        .bind(one_hour_ago) // created_at: the job genuinely started an hour ago
        .bind(now)
        .bind(now) // last_transition_time: recently entered this state, not "stuck"
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &ctx).await.unwrap();

        let actual_cost: Option<f64> =
            sqlx::query_scalar("SELECT actual_cost FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // Real elapsed duration was ~1 hour -- same cost as the 1-hour
        // estimate test above, computed from real wall-clock time now,
        // not the (here, absent) activeDeadlineSeconds.
        assert!((actual_cost.unwrap() - 0.004464).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_container_running_nonzero_exit_code_advances_to_failed() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/exit-code", ("1", 0));
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert("podman logs abc123", ("boom\n", 0));
        let (host, port) = crate::ssh::tests::mock_ssh_server(responses).await;
        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_port = port;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_ssh_ip(&pool, "ContainerRunning", &host).await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, exit_code): (String, Option<i64>) =
            sqlx::query_as("SELECT status, exit_code FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Failed");
        assert_eq!(exit_code, Some(1));

        // "Failed" must rejoin the normal cleanup path, same as "Succeeded".
        assert_eq!(next_state("Failed"), Some("VolumeDetaching"));
    }

    #[tokio::test]
    async fn test_container_running_with_no_ssh_key_configured_advances_without_exit_code() {
        let (host, port) = crate::ssh::tests::mock_ssh_server(HashMap::new()).await;
        let mut ctx = mock_ctx(false);
        ctx.worker_ssh_port = port;
        ctx.worker_ssh_private_key = String::new();

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_ssh_ip(&pool, "ContainerRunning", &host).await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, exit_code): (String, Option<i64>) =
            sqlx::query_as("SELECT status, exit_code FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Succeeded");
        assert_eq!(exit_code, None);
    }

    #[tokio::test]
    async fn test_volume_detaching_with_no_tracked_volume_advances_immediately() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeDeleted");
    }

    #[tokio::test]
    async fn test_volume_detaching_real_success_detaches_deletes_and_advances() {
        let app = axum::Router::new()
            .route(
                "/1.3/server/:uuid/storage/detach",
                axum::routing::post(|| async { axum::Json(serde_json::json!({"server": {}})) }),
            )
            .route(
                "/1.3/storage/:uuid",
                axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
            );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VolumeDetaching", "srv1").await;
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol-real-1', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VolumeDeleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_volume_detaching_tolerates_detach_failure_and_still_deletes() {
        // No /storage/attach|detach route registered at all -- the detach
        // call 404s, which must be logged and ignored, not block delete.
        let app = axum::Router::new().route(
            "/1.3/storage/:uuid",
            axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VolumeDetaching", "srv1").await;
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol-real-1', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1, "a detach failure must not block delete_volume");
    }

    #[tokio::test]
    async fn test_volume_detaching_dry_run_does_not_delete_tracked_row() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO job_volumes (id, job_id, size_gb, storage_class_name, provider_volume_id, created_at) \
             VALUES ('jv1', 'j1', 1, 'kube-shim-standard', 'vol-abc', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        advance_all(&pool, &mock_ctx(true)).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_volumes WHERE job_id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "dry-run must not touch a real-looking tracked row"
        );
    }

    #[tokio::test]
    async fn test_vm_terminating_with_no_vm_advances_immediately() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VMTerminating', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "Archived");
    }

    #[tokio::test]
    async fn test_vm_terminating_real_success_deletes_and_advances() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "stopped"}}))
            })
            .delete(|| async { axum::http::StatusCode::NO_CONTENT }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMTerminating", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "Archived");
    }

    #[tokio::test]
    async fn test_vm_terminating_stops_a_running_server_before_deleting_it() {
        // Phase 11's real live finding: UpCloud refuses delete_server on a
        // server that's still "started" (409 SERVER_STATE_ILLEGAL). This
        // tick must call stop_server instead of delete_server, and must
        // NOT advance the job yet -- deletion only happens once a later
        // tick observes "stopped".
        let delete_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delete_called_clone = delete_called.clone();
        let stop_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_called_clone = stop_called.clone();
        let app = axum::Router::new()
            .route(
                "/1.3/server/:uuid",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "started"}}))
                })
                .delete(move || {
                    delete_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async { axum::http::StatusCode::NO_CONTENT }
                }),
            )
            .route(
                "/1.3/server/:uuid/stop",
                axum::routing::post(move || {
                    stop_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async {
                        axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "started"}}))
                    }
                }),
            );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMTerminating", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(
            advanced, 0,
            "must not advance until the server is actually stopped"
        );
        assert!(stop_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(
            !delete_called.load(std::sync::atomic::Ordering::SeqCst),
            "must not call delete_server while the server is still started"
        );

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VMTerminating");
    }

    #[tokio::test]
    async fn test_vm_terminating_waits_while_server_is_stopping() {
        // An intermediate state (neither "started" nor "stopped" yet) must
        // not re-issue stop_server, and must not advance.
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "stopping"}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMTerminating", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "VMTerminating");
    }

    // --- Phase 11: activeDeadlineSeconds / stuck-state enforcement ---

    #[test]
    fn test_is_pre_completion_state_covers_the_working_states_only() {
        for state in [
            "Created",
            "VolumePending",
            "VolumeCreating",
            "VolumeCreated",
            "VMPending",
            "FirewallApplying",
            "FirewallVerified",
            "VMCreating",
            "VMRunning",
            "VolumeAttaching",
            "VolumeAttached",
            "ContainerRunning",
        ] {
            assert!(is_pre_completion_state(state), "{state} should be covered");
        }
        for state in [
            "Succeeded",
            "Failed",
            "VolumeDetaching",
            "VolumeDeleted",
            "VMTerminating",
            "Archived",
            "SomeUnknownState",
        ] {
            assert!(
                !is_pre_completion_state(state),
                "{state} should not be covered"
            );
        }
    }

    async fn insert_job_with_deadline(
        pool: &SqlitePool,
        status: &str,
        created_at: i64,
        last_transition_time: i64,
        active_deadline_seconds: i64,
    ) {
        let spec = serde_json::json!({
            "activeDeadlineSeconds": active_deadline_seconds,
            "template": {"spec": {"containers": [{"image": "busybox:latest"}]}}
        })
        .to_string();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', ?, ?, ?, ?, ?, 1)",
        )
        .bind(spec)
        .bind(status)
        .bind(created_at)
        .bind(created_at)
        .bind(last_transition_time)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_exceeded_forces_failed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        // Created 100s ago with a 60s deadline -- already exceeded, even
        // though this particular state (VolumeAttached, a mock
        // passthrough) hasn't itself been "stuck" for long at all.
        insert_job_with_deadline(&pool, "VolumeAttached", now - 100, now - 5, 60).await;

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, last_error): (String, Option<String>) =
            sqlx::query_as("SELECT status, last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Failed");
        assert!(last_error.unwrap().contains("DeadlineExceeded"));

        let (reason, event_type, message): (String, String, String) =
            sqlx::query_as("SELECT reason, type, message FROM events WHERE job_id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason, "Failed");
        assert_eq!(event_type, "Warning");
        assert!(message.contains("DeadlineExceeded"));
    }

    #[tokio::test]
    async fn test_normal_transition_records_a_normal_event() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "VMTerminating", "srv1").await;

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let (reason, event_type): (String, String) =
            sqlx::query_as("SELECT reason, type FROM events WHERE job_id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(reason, "Archived");
        assert_eq!(event_type, "Normal");
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_not_yet_exceeded_advances_normally() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        // Created 10s ago with a 3600s deadline -- nowhere close.
        insert_job_with_deadline(&pool, "VolumeAttached", now - 10, now - 5, 3600).await;

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "ContainerRunning");
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_does_not_apply_once_in_cleanup() {
        // A job whose deadline passed ages ago but is already cleaning up
        // (VolumeDetaching, past Succeeded/Failed) must not be yanked back
        // to "Failed" -- is_pre_completion_state excludes this state.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        insert_job_with_deadline(&pool, "VolumeDetaching", now - 100_000, now - 100_000, 60).await;

        let advanced = advance_all(&pool, &mock_ctx(true)).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            status, "VolumeDeleted",
            "normal cleanup must proceed untouched"
        );
    }

    #[tokio::test]
    async fn test_stuck_one_shot_state_times_out_to_failed() {
        // VolumePending, repeatedly failing against a real provider error,
        // for far longer than RETRY_ESCALATION_THRESHOLD -- must give up
        // rather than retry forever (the gap Phase 8/9 explicitly left
        // open for Phase 11 to close).
        let app = axum::Router::new().route(
            "/1.3/storage",
            axum::routing::post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        // No activeDeadlineSeconds this time -- only the per-state stuck
        // timeout should fire.
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, last_transition_time, retry_count, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, ?, 40, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now - 1000)
        .bind(now - 1000)
        .bind(now - (RETRY_ESCALATION_THRESHOLD + 1))
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let (status, last_error): (String, Option<String>) =
            sqlx::query_as("SELECT status, last_error FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "Failed");
        assert!(last_error.unwrap().contains("StateTimeout"));
    }

    #[tokio::test]
    async fn test_stuck_polling_state_times_out_to_failed() {
        // FirewallVerified, never seeing the expected rules appear, for
        // far longer than RETRY_ESCALATION_THRESHOLD.
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"firewall_rules": {"firewall_rule": []}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, created_at, updated_at, last_transition_time, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'FirewallVerified', 'srv1', ?, ?, ?, 1)",
        )
        .bind(now - 1000)
        .bind(now - 1000)
        .bind(now - (RETRY_ESCALATION_THRESHOLD + 1))
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);

        let status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = 'j1'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "Failed");
    }

    #[tokio::test]
    async fn test_volume_pending_reuses_existing_untracked_volume_instead_of_duplicating() {
        // Simulates a crash between create_volume() succeeding and
        // insert_job_volume() committing: the real volume already exists
        // (with the exact expected title) but nothing tracks it yet.
        let create_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let create_called_clone = create_called.clone();
        let app = axum::Router::new()
            .route(
                "/1.3/storage/normal",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"storages": {"storage": [
                        {"uuid": "vol-existing", "size": 1, "tier": "maxiops", "title": "kube-shim-test-vol-job-one", "zone": "de-fra1"}
                    ]}}))
                }),
            )
            .route(
                "/1.3/storage",
                axum::routing::post(move || {
                    create_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }
                }),
            );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
        assert!(
            !create_called.load(std::sync::atomic::Ordering::SeqCst),
            "must reuse the existing volume, never call create_volume"
        );

        let provider_volume_id: String =
            sqlx::query_scalar("SELECT provider_volume_id FROM job_volumes WHERE job_id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(provider_volume_id, "vol-existing");
    }

    #[tokio::test]
    async fn test_vm_pending_reuses_existing_untracked_server_instead_of_duplicating() {
        let create_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let create_called_clone = create_called.clone();
        let app = axum::Router::new()
            .route(
                "/1.3/server",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"servers": {"server": [
                        {"uuid": "srv-existing", "title": "kube-shim-test-worker-job-one", "state": "started", "zone": "de-fra1"}
                    ]}}))
                })
                .post(move || {
                    create_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }
                }),
            );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', ?, ?, 1)",
        )
        .bind(ephemeral_volume_spec())
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
        assert!(
            !create_called.load(std::sync::atomic::Ordering::SeqCst),
            "must reuse the existing server, never call create_server"
        );

        let worker_vm_id: Option<String> =
            sqlx::query_scalar("SELECT worker_vm_id FROM jobs WHERE id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(worker_vm_id.as_deref(), Some("srv-existing"));
    }
}
