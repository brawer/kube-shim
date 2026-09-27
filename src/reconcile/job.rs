//! Job state machine. Phase 6 built it fully mocked; Phase 8 made volume
//! create/delete real; Phase 9 makes the rest of the pipeline real too --
//! real worker VMs, real firewall rules, real volume attachment. Only
//! `Succeeded` staying unconditional (never a real `Failed`) and
//! `exit_code` staying unpopulated remain honestly mocked: both need the
//! job's real exit status, which lives in `/tmp/job-status.txt` inside the
//! worker VM and isn't fetchable until `src/ssh.rs` exists (Phase 10).
//!
//! **Three real findings from building this, all changing the pipeline's
//! shape from what earlier phases assumed:**
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
//! 3. **Completion detection without SSH.** With no SSH client until
//!    Phase 10, the shim can't read the worker's status file. Instead,
//!    the cloud-init script's own last action is `poweroff` once the
//!    container exits (see `bootstrap/cloud-init-template.sh`) --
//!    `ContainerRunning`'s handler polls `get_server` and treats the
//!    transition away from `"started"` as "the job is done". Real,
//!    SSH-free, and it's a primitive the shim already had (Phase 7).
//!
//! **A fourth, smaller finding:** `VolumeDetaching`'s real handler now
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
//! `retry_count`/`last_error`. The real gap this leaves (documented, not
//! fixed here): a resource that never *does* reach its target state polls
//! silently forever, with no timeout. That's explicitly Phase 11's job
//! ("Reconciliation Hardening" already lists volume/VM/firewall timeouts
//! as its own deliverable) -- see this module's own precedent from Phase
//! 8, which left the equivalent gap for one-shot actions the same way.

use crate::providers::{
    CloudProvider, CreateServerRequest, CreateVolumeRequest, FirewallAction, FirewallDirection,
    FirewallFamily, FirewallRule,
};
use crate::{cloud_init, volumes, workload};
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
/// way around). Minus `BudgetWait` (Phase 13 doesn't exist yet) and
/// collapsing the real `Succeeded`/`Failed` fork down to always
/// `Succeeded` (no real exit-code visibility yet -- Phase 10's job).
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
}

/// The state one tick after `current`, or `None` if `current` is
/// `TERMINAL_STATE` or not a state this pipeline recognizes at all (e.g.
/// leftover data from a different schema version -- callers should leave
/// such a job alone and log a warning rather than guess).
pub fn next_state(current: &str) -> Option<&'static str> {
    let index = STATE_SEQUENCE.iter().position(|state| *state == current)?;
    STATE_SEQUENCE.get(index + 1).copied()
}

/// Advances every job not yet in `TERMINAL_STATE` by up to one state.
/// States with no real work (see `STATE_SEQUENCE`'s own docs) advance
/// unconditionally; every other state only advances once its real work
/// actually succeeds (a one-shot action) or its polled target is actually
/// reached (a polling state) -- otherwise the job stays put and tries
/// again next tick. Returns how many jobs were advanced (for
/// logging/testing).
pub async fn advance_all(pool: &SqlitePool, ctx: &JobContext) -> Result<usize> {
    let rows = sqlx::query(
        "SELECT id, name, namespace, status, spec, last_transition_time FROM jobs WHERE status != ?",
    )
    .bind(TERMINAL_STATE)
    .fetch_all(pool)
    .await?;

    let mut advanced = 0;
    for row in rows {
        let id: String = row.get(0);
        let name: String = row.get(1);
        let namespace: String = row.get(2);
        let status: String = row.get(3);
        let spec_str: String = row.get(4);
        let last_transition_time: Option<i64> = row.get(5);

        let Some(next) = next_state(&status) else {
            tracing::warn!(
                "job {namespace}/{name} is in unrecognized state {status:?}, leaving it alone"
            );
            continue;
        };

        let should_advance = match status.as_str() {
            "VolumePending" => {
                handle_volume_pending(
                    pool,
                    ctx,
                    &id,
                    &namespace,
                    &name,
                    &spec_str,
                    last_transition_time,
                )
                .await?
            }
            "VMPending" => {
                handle_vm_pending(
                    pool,
                    ctx,
                    &id,
                    &namespace,
                    &name,
                    &spec_str,
                    last_transition_time,
                )
                .await?
            }
            "FirewallApplying" => {
                handle_firewall_applying(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
            }
            "FirewallVerified" => {
                handle_firewall_verified(pool, ctx, &id, &namespace, &name).await?
            }
            "VMCreating" => handle_vm_creating(pool, ctx, &id, &namespace, &name).await?,
            "VolumeAttaching" => {
                handle_volume_attaching(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
            }
            "ContainerRunning" => {
                handle_container_running(pool, ctx, &id, &namespace, &name).await?
            }
            "VolumeDetaching" => {
                handle_volume_detaching(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
            }
            "VMTerminating" => {
                handle_vm_terminating(pool, ctx, &id, &namespace, &name, last_transition_time)
                    .await?
            }
            _ => true,
        };

        if !should_advance {
            continue;
        }

        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
            UPDATE jobs
            SET status = ?, last_transition_time = ?, updated_at = ?, version = version + 1
            WHERE id = ?
            "#,
        )
        .bind(next)
        .bind(now)
        .bind(now)
        .bind(&id)
        .execute(pool)
        .await?;

        tracing::info!("job {namespace}/{name}: {status} -> {next}");
        advanced += 1;
    }

    Ok(advanced)
}

/// Job `spec` (as stored by `reconcile::schedule::create_job_run`) is the
/// CronJob's `jobTemplate.spec` directly -- `template.spec.volumes[]`,
/// `template.spec.containers[]`, `activeDeadlineSeconds` all live at the
/// top level here, one JSON path segment shorter than in the CronJob's
/// own spec (see `src/admission.rs` for that longer form).
fn find_ephemeral_volume(spec: &JsonValue) -> Option<&JsonValue> {
    spec.pointer("/template/spec/volumes")
        .and_then(JsonValue::as_array)?
        .iter()
        .find_map(|v| v.pointer("/ephemeral/volumeClaimTemplate/spec"))
}

fn extract_resource_requests(spec: &JsonValue) -> (Option<u32>, Option<u32>) {
    let requests = spec.pointer("/template/spec/containers/0/resources/requests");
    let cpu_cores = requests
        .and_then(|r| r.get("cpu"))
        .and_then(JsonValue::as_str)
        .and_then(|q| workload::parse_cpu_cores(q).ok());
    let memory_gb = requests
        .and_then(|r| r.get("memory"))
        .and_then(JsonValue::as_str)
        .and_then(|q| volumes::parse_storage_quantity_gb(q).ok());
    (cpu_cores, memory_gb)
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
    let (cpu_cores, memory_gb) = extract_resource_requests(&spec);
    let plan =
        workload::smallest_fitting_server_plan(cpu_cores.unwrap_or(1), memory_gb.unwrap_or(1));

    let Some(plan) = plan else {
        let msg = format!(
            "no known server plan is big enough for {}/{} CPU/GB requested",
            cpu_cores.unwrap_or(1),
            memory_gb.unwrap_or(1)
        );
        record_failed_attempt(pool, job_id, namespace, name, &msg, last_transition_time).await?;
        return Ok(false);
    };

    if ctx.dry_run {
        tracing::info!(
            "DRY-RUN: would launch VM for job {namespace}/{name}: plan {} \
             ({cpu_cores:?} CPU / {memory_gb:?}GB requested)",
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
        Ok(server) if server.state == "started" => {
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
/// the shim to *do* here, only to notice when it's done. Polls
/// `get_server` and treats the worker VM shutting itself down (cloud-
/// init's own last action, see `bootstrap/cloud-init-template.sh`) as the
/// real, SSH-free completion signal. A polling state: still running is
/// logged at `debug`, never a failed attempt.
async fn handle_container_running(
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
        Ok(server) if server.state != "started" => {
            tracing::info!(
                "job {namespace}/{name}: worker VM {vm_id} is {} (container finished)",
                server.state
            );
            Ok(true)
        }
        Ok(_) => {
            tracing::debug!("job {namespace}/{name}: container still running on {vm_id}");
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

    fn mock_ctx(dry_run: bool) -> JobContext {
        JobContext {
            provider: Arc::new(UpCloudProvider::new("unused-in-dry-run")),
            dry_run,
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
            worker_template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            worker_ssh_public_keys: vec!["ssh-ed25519 AAAA test".to_string()],
            own_public_ip: Some("203.0.113.5".to_string()),
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Created', 0, 0, 1)",
        )
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{\"template\":{\"spec\":{}}}', 'VolumePending', 0, 0, 1)",
        )
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VolumePending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
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
    async fn test_vm_pending_refuses_with_no_ssh_keys_configured() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', 0, 0, 1)",
        )
        .bind(ephemeral_volume_spec())
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', ?, 'VMPending', 0, 0, 1)",
        )
        .bind(spec)
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
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_id, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', ?, ?, 0, 0, 1)",
        )
        .bind(status)
        .bind(vm_id)
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

    #[tokio::test]
    async fn test_container_running_waits_while_started() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "started"}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "ContainerRunning", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 0);
    }

    #[tokio::test]
    async fn test_container_running_advances_once_vm_stops_itself() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"server": {"uuid": "srv1", "title": "t", "state": "stopped"}}))
            }),
        );
        let provider = crate::providers::upcloud::tests::mock_server(app).await;
        let mut ctx = mock_ctx(false);
        ctx.provider = Arc::new(provider);

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "ContainerRunning", "srv1").await;

        let advanced = advance_all(&pool, &ctx).await.unwrap();
        assert_eq!(advanced, 1);
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
            axum::routing::delete(|| async { axum::http::StatusCode::NO_CONTENT }),
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
}
