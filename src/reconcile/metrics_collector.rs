//! Real worker-VM metrics, collected over SSH -- replaces `api::metrics`'
//! original design, which only ever estimated usage from a job's own
//! `resources.requests` (the plan's own capacity table, not anything
//! actually measured). "I'd like actual metrics, not fake numbers" --
//! this is that.
//!
//! One combined SSH round-trip per job, every
//! `reconcile::METRICS_POLL_INTERVAL`: `podman stats --no-stream
//! --format json` (verified hands-on against both a local podman 6.1.2
//! and the real apt-installed podman 4.9.3 this project's own worker
//! VMs actually run -- same JSON schema on both), plus
//! `free -b`/`nproc`/`/proc/loadavg` for the machine as a whole.
//!
//! **Polled from `VMRunning` onward, not just `ContainerRunning`.**
//! `worker_ssh_ip` is already set by the time a job reaches `VMRunning`
//! (`handle_vm_creating`), and firewall rules are verified *before*
//! that (`FirewallVerified` precedes `VMCreating` in `STATE_SEQUENCE`)
//! -- so SSH is genuinely reachable for `VMRunning`/`VolumeAttaching`/
//! `VolumeAttached` too, not just `ContainerRunning`. Collecting from
//! `VMRunning` onward means a job stuck mid-provisioning (a volume
//! attach retry loop, say) still gets real node-level visibility --
//! exactly the case where "is the machine itself healthy" is most
//! worth knowing. `job::is_pre_completion_state` (combined with
//! `worker_ssh_ip IS NOT NULL`, which self-excludes every earlier
//! state) is reused rather than hardcoding that state list a second
//! time -- it already excludes the cleanup tail for the same reason
//! `api::logs` already established: `worker_ssh_ip` is never cleared
//! once set, but the worker VM it pointed at is deleted a few states
//! later, and this module's host-key verification accepts any key --
//! see `src/ssh.rs`'s own docs.
//!
//! **A sample's container stats and node-health are collected and
//! recorded independently, not all-or-nothing.** `podman stats` with
//! no container ID returns every *currently running* container as a
//! JSON array -- zero entries (nothing started yet), one (today's
//! only real case), or, if this project ever supports more than one
//! container per pod, more than one; `Sample.containers` is a `Vec`
//! for exactly that reason, not because multiple containers are
//! supported anywhere else yet (`cloud_init`/`cloud-init-template.sh`
//! still only ever start one). A job with zero running containers
//! (still provisioning) still gets its *node* sample recorded -- Node-
//! level serving shouldn't wait on Pod-level readiness any more than it
//! does on a real cluster, where Node metrics are already independent
//! of what's scheduled on it.
//!
//! Deliberately "latest sample, upserted" (`worker_metrics`), not a
//! time series -- real Kubernetes metrics-server itself only ever
//! serves the most recent window too, never history.

use crate::reconcile::job::is_pre_completion_state;
use crate::ssh::{self, WorkerSshConfig};
use anyhow::Result;
use chrono::Utc;
use sqlx::{Row, SqlitePool};
use std::time::Duration;

/// Bounds one job's *entire* SSH round-trip, not just the connect phase
/// `src/ssh.rs`'s own `CONNECT_TIMEOUT`/`INACTIVITY_TIMEOUT` already
/// cover. Those bound the transport (connect+handshake; "has this gone
/// completely silent") but not the command itself -- a worker that
/// accepts the connection and then hangs mid-command (`podman` wedged,
/// stuck disk I/O, ...) while the session isn't *fully* silent could
/// otherwise block `ssh::exec_once` forever. That matters more here
/// than anywhere else SSH is used in this project: `poll_once` awaits
/// every job's round-trip *sequentially*, so one stuck worker without
/// this wouldn't just leave that job's own sample stale -- it would
/// block every other job's poll forever too, since `run_metrics_poll_loop`
/// never ticks again until `poll_once` itself returns. Comfortably under
/// `INACTIVITY_TIMEOUT` (30s): if the connection truly goes silent,
/// `ssh::exec_once` would eventually error out on its own regardless,
/// but this should fire well before that in practice.
const SSH_EXEC_TIMEOUT: Duration = Duration::from_secs(20);

/// One shell round-trip: the job's own container's stats, then the
/// machine's overall memory/CPU/load, separated by a marker that can't
/// collide with any of their own real output.
const SEP: &str = "___KUBESHIM_METRICS_SEP___";

fn collection_command() -> String {
    format!(
        "podman stats --no-stream --format json 2>/dev/null; \
         echo '{SEP}'; \
         free -b | awk '/^Mem:/ {{print $2, $3}}'; \
         echo '{SEP}'; \
         nproc; \
         echo '{SEP}'; \
         cat /proc/loadavg"
    )
}

/// One currently-running container's own stats -- `name` is always
/// `"main"` today (`cloud_init`'s own one-container-per-pod
/// convention, matching `api::metrics`'s existing `ContainerMetrics`),
/// but comes from `podman stats`' real `name` field, not hardcoded,
/// so this doesn't need revisiting if that ever changes.
#[derive(Debug, PartialEq)]
struct ContainerSample {
    name: String,
    cpu_millicores: u32,
    memory_usage_bytes: u64,
}

#[derive(Debug, PartialEq)]
struct Sample {
    /// Empty when nothing's running yet (still provisioning) -- see
    /// this module's own top-level docs on why that's recorded as "no
    /// container data" rather than discarding the whole sample.
    containers: Vec<ContainerSample>,
    node_memory_total_bytes: u64,
    node_memory_used_bytes: u64,
    node_cpu_count: u32,
    node_load1: f64,
}

/// `podman stats`' own human-readable byte formatting (`go-units`'
/// `HumanSize`: decimal, `B`/`kB`/`MB`/`GB`/`TB`) -- *not* the same
/// alphabet `volumes::parse_storage_quantity_mb` parses (Kubernetes'
/// `Ki`/`Mi`/`Gi`, binary). Verified hands-on: a container started with
/// `--memory=512m` (binary mebibytes) reports its limit back as
/// `"536.9MB"` (decimal megabytes of the same real byte count) --
/// confirming podman's own CLI output is always decimal, regardless of
/// which convention the limit was originally expressed in.
fn parse_podman_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    let (number, multiplier): (&str, f64) = if let Some(n) = s.strip_suffix("PB") {
        (n, 1e15)
    } else if let Some(n) = s.strip_suffix("TB") {
        (n, 1e12)
    } else if let Some(n) = s.strip_suffix("GB") {
        (n, 1e9)
    } else if let Some(n) = s.strip_suffix("MB") {
        (n, 1e6)
    } else if let Some(n) = s.strip_suffix("kB") {
        (n, 1e3)
    } else {
        (s.strip_suffix('B')?, 1.0)
    };
    let value: f64 = number.trim().parse().ok()?;
    Some((value * multiplier).round() as u64)
}

/// Every entry in `podman stats --format json`'s own array -- e.g.
/// `[{"name": "x", "cpu_percent": "0.26%", "mem_usage": "303.1kB / 8.289GB", ...}]`.
/// Zero entries (nothing running yet) is a real, valid outcome, not a
/// parse failure -- returns an empty `Vec`, not `None`; `None` is
/// reserved for output that doesn't even look like `podman stats`
/// JSON at all (garbage, a truncated/failed command). `cpu_percent` is
/// already relative to one full core (podman/docker's own
/// long-standing convention: `"100%"` means one core fully saturated,
/// not "100% of all cores"), so converting to millicores is a direct
/// `* 10`, not a core-count-dependent calculation. One malformed entry
/// is skipped rather than failing the whole list -- `filter_map`, not
/// `collect::<Option<Vec<_>>>()` -- so a transient oddity on one
/// container doesn't hide every other one's perfectly good data.
fn parse_podman_stats(json: &str) -> Option<Vec<ContainerSample>> {
    let parsed: serde_json::Value = serde_json::from_str(json).ok()?;
    let entries = parsed.as_array()?;

    Some(
        entries
            .iter()
            .filter_map(|entry| {
                let name = entry.get("name")?.as_str()?.to_string();
                let cpu_percent: f64 = entry
                    .get("cpu_percent")?
                    .as_str()?
                    .trim_end_matches('%')
                    .parse()
                    .ok()?;
                let mem_usage = entry.get("mem_usage")?.as_str()?;
                let used = mem_usage.split('/').next()?;
                let memory_usage_bytes = parse_podman_bytes(used)?;
                let cpu_millicores = (cpu_percent * 10.0).round() as u32;
                Some(ContainerSample {
                    name,
                    cpu_millicores,
                    memory_usage_bytes,
                })
            })
            .collect(),
    )
}

/// The combined command's own three remaining sections: `free -b`'s
/// `Mem:` line (already reduced to just `total used` by the remote
/// `awk` call), `nproc`'s bare core count, and `/proc/loadavg`'s first
/// field.
fn parse_node_health(
    free_line: &str,
    nproc_line: &str,
    loadavg_line: &str,
) -> Option<(u64, u64, u32, f64)> {
    let mut parts = free_line.split_whitespace();
    let total: u64 = parts.next()?.parse().ok()?;
    let used: u64 = parts.next()?.parse().ok()?;
    let cpu_count: u32 = nproc_line.trim().parse().ok()?;
    let load1: f64 = loadavg_line.split_whitespace().next()?.parse().ok()?;
    Some((total, used, cpu_count, load1))
}

/// `containers` being empty is a perfectly valid, recordable outcome
/// (nothing running yet) -- only a genuinely unparseable
/// `podman stats` section (`parse_podman_stats` returning `None`, not
/// `Some(vec![])`) or unparseable node-health fails the *whole*
/// sample; those two are independent precisely so a job with no
/// container yet still gets its real node-health recorded.
fn parse_sample(stdout: &str) -> Option<Sample> {
    let mut sections = stdout.split(SEP);
    let stats_section = sections.next()?.trim();
    let free_section = sections.next()?.trim();
    let nproc_section = sections.next()?.trim();
    let loadavg_section = sections.next()?.trim();

    let containers = parse_podman_stats(stats_section)?;
    let (node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1) =
        parse_node_health(free_section, nproc_section, loadavg_section)?;

    Some(Sample {
        containers,
        node_memory_total_bytes,
        node_memory_used_bytes,
        node_cpu_count,
        node_load1,
    })
}

/// `cpu_millicores`/`memory_usage_bytes` are nullable: `NULL` means
/// exactly what an empty `sample.containers` means -- no container
/// running yet, not zero usage. Storage only ever keeps the *first*
/// container's stats (today, there's never more than one); see this
/// module's own top-level docs on why `Sample.containers` is still a
/// `Vec` despite that -- the collection/parsing layer is already
/// forward-compatible with more than one, storage deliberately isn't,
/// since actually persisting several would need a real child table,
/// disproportionate to a feature nothing in this project supports yet.
async fn upsert_sample(pool: &SqlitePool, job_id: &str, sample: &Sample) -> Result<()> {
    let now = Utc::now().timestamp();
    let container = sample.containers.first();
    let cpu_millicores = container.map(|c| c.cpu_millicores);
    let memory_usage_bytes = container.map(|c| c.memory_usage_bytes as i64);

    sqlx::query(
        "INSERT INTO worker_metrics \
         (job_id, cpu_millicores, memory_usage_bytes, node_memory_total_bytes, \
          node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(job_id) DO UPDATE SET \
             cpu_millicores = excluded.cpu_millicores, \
             memory_usage_bytes = excluded.memory_usage_bytes, \
             node_memory_total_bytes = excluded.node_memory_total_bytes, \
             node_memory_used_bytes = excluded.node_memory_used_bytes, \
             node_cpu_count = excluded.node_cpu_count, \
             node_load1 = excluded.node_load1, \
             sampled_at = excluded.sampled_at",
    )
    .bind(job_id)
    .bind(cpu_millicores)
    .bind(memory_usage_bytes)
    .bind(sample.node_memory_total_bytes as i64)
    .bind(sample.node_memory_used_bytes as i64)
    .bind(sample.node_cpu_count)
    .bind(sample.node_load1)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes any sample for a job that's no longer `ContainerRunning` --
/// `api::metrics`/`api::nodes` must never serve a stale reading for a
/// job whose worker VM may already be gone, and without this,
/// `worker_metrics` would otherwise grow by one permanent row per job
/// ever run, forever. Run at the start of every poll (not just when a
/// job happens to transition during this exact tick), so a job that
/// left `ContainerRunning` between polls is cleaned up within one
/// `METRICS_POLL_INTERVAL`, not left stale indefinitely.
async fn forget_stale_samples(pool: &SqlitePool) -> Result<()> {
    // Filtered in Rust via is_pre_completion_state, same as the SELECT
    // below, rather than a hardcoded SQL state list that could drift
    // from it. A LEFT JOIN (not INNER) so a row whose job has somehow
    // vanished entirely (status is NULL here) is also cleaned up, not
    // silently kept forever.
    let rows = sqlx::query(
        "SELECT worker_metrics.job_id, jobs.status \
         FROM worker_metrics \
         LEFT JOIN jobs ON worker_metrics.job_id = jobs.id",
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let job_id: String = row.get(0);
        let status: Option<String> = row.get(1);
        let still_valid = status.is_some_and(|s| is_pre_completion_state(&s));
        if !still_valid {
            sqlx::query("DELETE FROM worker_metrics WHERE job_id = ?")
                .bind(&job_id)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

/// One polling pass: every job currently `ContainerRunning` with a
/// known `worker_ssh_ip` gets one SSH round-trip, bounded by
/// `SSH_EXEC_TIMEOUT`; a job whose SSH call fails or times out (worker
/// not reachable *yet*, e.g. firewall rules only just applied; or
/// genuinely hung) or whose output doesn't parse just keeps its last
/// known sample (if any) rather than being zeroed out or removed -- a
/// single missed poll isn't a reason to make `kubectl top` go blank for
/// 30s, and critically, isn't a reason for *every other job's* poll to
/// never happen either. Returns how many samples were successfully
/// updated.
pub async fn poll_once(pool: &SqlitePool, ssh_config: &WorkerSshConfig) -> Result<usize> {
    poll_once_with_timeout(pool, ssh_config, SSH_EXEC_TIMEOUT).await
}

/// Split out from `poll_once` purely so a test can pass a short timeout
/// and verify a hung SSH round-trip actually gets cut off (and that the
/// rest of the pass still proceeds), rather than paying the real
/// `SSH_EXEC_TIMEOUT` (20s) on every test run just to prove the wrapper
/// works -- same reasoning as `src/ssh.rs`'s own
/// `connect_and_open_channel_with_timeout` split.
async fn poll_once_with_timeout(
    pool: &SqlitePool,
    ssh_config: &WorkerSshConfig,
    ssh_timeout: Duration,
) -> Result<usize> {
    forget_stale_samples(pool).await?;

    // worker_ssh_ip IS NOT NULL alone already excludes every state
    // before VMRunning (it isn't set yet); is_pre_completion_state
    // excludes the cleanup tail. Together they resolve to exactly
    // "VMRunning through ContainerRunning" -- see this module's own
    // top-level docs.
    let rows =
        sqlx::query("SELECT id, status, worker_ssh_ip FROM jobs WHERE worker_ssh_ip IS NOT NULL")
            .fetch_all(pool)
            .await?;

    let command = collection_command();
    let mut updated = 0;
    for row in rows {
        let job_id: String = row.get(0);
        let status: String = row.get(1);
        if !is_pre_completion_state(&status) {
            continue;
        }
        let ip: String = row.get(2);

        let exec = ssh::exec_once(&ip, ssh_config.port, &ssh_config.private_key, &command);
        let output = match tokio::time::timeout(ssh_timeout, exec).await {
            Ok(Ok(output)) => output,
            Ok(Err(err)) => {
                tracing::debug!(
                    "metrics poll: SSH to {ip} for job {job_id} failed, will retry: {err}"
                );
                continue;
            }
            Err(_) => {
                tracing::debug!(
                    "metrics poll: SSH to {ip} for job {job_id} did not complete within \
                     {ssh_timeout:?}, will retry"
                );
                continue;
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let Some(sample) = parse_sample(&stdout) else {
            tracing::debug!("metrics poll: could not parse output from {ip} for job {job_id}");
            continue;
        };

        if let Err(err) = upsert_sample(pool, &job_id, &sample).await {
            tracing::error!("metrics poll: failed to record sample for job {job_id}: {err:?}");
            continue;
        }
        updated += 1;
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_podman_bytes_decimal_suffixes() {
        assert_eq!(parse_podman_bytes("294.9kB"), Some(294_900));
        assert_eq!(parse_podman_bytes("536.9MB"), Some(536_900_000));
        assert_eq!(parse_podman_bytes("8.289GB"), Some(8_289_000_000));
        assert_eq!(parse_podman_bytes("0B"), Some(0));
    }

    #[test]
    fn test_parse_podman_bytes_rejects_unknown_suffix() {
        assert_eq!(parse_podman_bytes("1Ki"), None);
    }

    #[test]
    fn test_parse_podman_stats_real_shape() {
        // Exact shape verified hands-on against both a local podman
        // 6.1.2 and the real apt-installed podman 4.9.3 this project's
        // own worker VMs run.
        let json = r#"[
         {
          "id": "a1afb897420e",
          "name": "stats-verify2",
          "cpu_time": "5.332ms",
          "cpu_percent": "0.12%",
          "avg_cpu": "0.12%",
          "mem_usage": "294.9kB / 536.9MB",
          "mem_percent": "0.05%",
          "net_io": "1.22kB / 558B",
          "block_io": "0B / 0B",
          "pids": "1"
         }
        ]"#;
        let containers = parse_podman_stats(json).unwrap();
        assert_eq!(containers.len(), 1);
        assert_eq!(containers[0].name, "stats-verify2");
        assert_eq!(containers[0].cpu_millicores, 1); // 0.12% * 10, rounded
        assert_eq!(containers[0].memory_usage_bytes, 294_900);
    }

    #[test]
    fn test_parse_podman_stats_empty_array_is_an_empty_vec_not_none() {
        // What zero running containers produces (nothing started yet) --
        // a real, valid outcome, not a parse failure: podman succeeded
        // and correctly reported "nothing running."
        assert_eq!(parse_podman_stats("[]"), Some(vec![]));
    }

    #[test]
    fn test_parse_podman_stats_garbage_is_none() {
        assert!(parse_podman_stats("not json at all").is_none());
    }

    #[test]
    fn test_parse_node_health_real_shape() {
        // Exact shape verified hands-on against kube-shim.brawer.ch's
        // own real Ubuntu VM.
        let (total, used, cpus, load1) =
            parse_node_health("889794560 479551488", "2", "0.52 0.58 0.59 2/458 12345").unwrap();
        assert_eq!(total, 889_794_560);
        assert_eq!(used, 479_551_488);
        assert_eq!(cpus, 2);
        assert_eq!(load1, 0.52);
    }

    #[test]
    fn test_parse_sample_full_round_trip() {
        let stats =
            r#"[{"name": "main", "cpu_percent": "25.00%", "mem_usage": "512.0MB / 1.0GB"}]"#;
        let stdout = format!(
            "{stats}\n{SEP}\n8000000000 4000000000\n{SEP}\n4\n{SEP}\n1.00 0.90 0.80 1/200 999\n"
        );
        let sample = parse_sample(&stdout).unwrap();
        assert_eq!(sample.containers.len(), 1);
        assert_eq!(sample.containers[0].cpu_millicores, 250);
        assert_eq!(sample.containers[0].memory_usage_bytes, 512_000_000);
        assert_eq!(sample.node_memory_total_bytes, 8_000_000_000);
        assert_eq!(sample.node_memory_used_bytes, 4_000_000_000);
        assert_eq!(sample.node_cpu_count, 4);
        assert_eq!(sample.node_load1, 1.0);
    }

    #[test]
    fn test_parse_sample_with_no_containers_still_records_node_health() {
        // The whole point of decoupling the two halves: a job with no
        // container running yet (still provisioning) must still get a
        // real, recordable sample for the machine itself.
        let stdout = format!(
            "[]\n{SEP}\n8000000000 4000000000\n{SEP}\n4\n{SEP}\n1.00 0.90 0.80 1/200 999\n"
        );
        let sample = parse_sample(&stdout).unwrap();
        assert!(sample.containers.is_empty());
        assert_eq!(sample.node_memory_total_bytes, 8_000_000_000);
        assert_eq!(sample.node_cpu_count, 4);
    }

    #[test]
    fn test_parse_sample_against_real_captured_output() {
        // Captured verbatim from running this exact combined command
        // over a real SSH connection against kube-shim.brawer.ch's own
        // real Ubuntu/podman 4.9.3 host (not a worker VM, but the same
        // OS/podman combination every worker VM runs) -- the strongest
        // confidence this module's parsing actually matches reality,
        // short of running it against a real worker.
        let stdout = "[\n \
         {\n  \"id\": \"d9e5f0b07f76\",\n  \"name\": \"systemd-kube-shim\",\n  \
         \"cpu_time\": \"5.428501s\",\n  \"cpu_percent\": \"0.02%\",\n  \
         \"avg_cpu\": \"0.02%\",\n  \"mem_usage\": \"1.839MB / 889.8MB\",\n  \
         \"mem_percent\": \"0.21%\",\n  \"net_io\": \"1.063MB / 221.9kB\",\n  \
         \"block_io\": \"0B / 0B\",\n  \"pids\": \"6\"\n }\n]\n\
         ___KUBESHIM_METRICS_SEP___\n\
         889794560 479375360\n\
         ___KUBESHIM_METRICS_SEP___\n\
         1\n\
         ___KUBESHIM_METRICS_SEP___\n\
         0.00 0.00 0.00 1/144 101837\n";

        let sample = parse_sample(stdout).unwrap();
        assert_eq!(sample.containers.len(), 1);
        assert_eq!(sample.containers[0].cpu_millicores, 0); // 0.02% rounds down to 0m
        assert_eq!(sample.containers[0].memory_usage_bytes, 1_839_000);
        assert_eq!(sample.node_memory_total_bytes, 889_794_560);
        assert_eq!(sample.node_memory_used_bytes, 479_375_360);
        assert_eq!(sample.node_cpu_count, 1);
        assert_eq!(sample.node_load1, 0.0);
    }

    #[test]
    fn test_parse_sample_missing_sections_is_none() {
        assert!(parse_sample("not enough separators here").is_none());
    }

    #[tokio::test]
    async fn test_poll_once_ignores_jobs_with_no_worker_ssh_ip_yet() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeAttaching', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: String::new(),
            port: 1, // nothing listens here; any attempt would fail fast
        };
        let updated = poll_once(&pool, &ssh_config).await.unwrap();
        assert_eq!(updated, 0);
    }

    /// Spawns a task that accepts exactly one TCP connection on
    /// `listener` and records whether it happened -- used below to
    /// distinguish "this job's SSH round-trip was genuinely attempted"
    /// from "attempted and merely failed," which a bare `updated == 0`
    /// can't tell apart on its own.
    fn watch_for_one_connection(
        listener: std::net::TcpListener,
    ) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        let connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let connected_clone = connected.clone();
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                connected_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        connected
    }

    #[tokio::test]
    async fn test_poll_once_includes_pre_container_running_states_once_ssh_ip_is_set() {
        // VolumeAttached: SSH-reachable (worker_ssh_ip set, firewall
        // already verified earlier in the sequence) but still short of
        // ContainerRunning -- must be attempted, not skipped, per this
        // module's own widened collection window.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeAttached', '127.0.0.1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connected = watch_for_one_connection(listener);

        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port,
        };
        let _ = poll_once_with_timeout(&pool, &ssh_config, Duration::from_millis(200)).await;

        assert!(
            connected.load(std::sync::atomic::Ordering::SeqCst),
            "VolumeAttached must be attempted -- it's SSH-reachable and pre-completion"
        );
    }

    #[tokio::test]
    async fn test_poll_once_excludes_cleanup_tail_states_despite_worker_ssh_ip_still_set() {
        // VolumeDetaching: worker_ssh_ip is never cleared once set, but
        // the worker VM it pointed at is deleted a few states later --
        // must NOT be attempted, same stale-IP reasoning api::logs
        // already established for this exact field.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeDetaching', '127.0.0.1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connected = watch_for_one_connection(listener);

        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port,
        };
        let _ = poll_once_with_timeout(&pool, &ssh_config, Duration::from_millis(200)).await;

        assert!(
            !connected.load(std::sync::atomic::Ordering::SeqCst),
            "VolumeDetaching must not be attempted even though worker_ssh_ip is still set"
        );
    }

    #[tokio::test]
    async fn test_poll_once_tolerates_unreachable_worker() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'ContainerRunning', '127.0.0.1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: String::new(),
            port: 1,
        };
        // Must not error out even though SSH itself will fail outright
        // (invalid key, unreachable port) -- a poll pass tolerates any
        // one job's failure and keeps going.
        let updated = poll_once(&pool, &ssh_config).await.unwrap();
        assert_eq!(updated, 0);
    }

    #[tokio::test]
    async fn test_poll_once_bounds_a_hung_worker_instead_of_blocking_forever() {
        // The property SSH_EXEC_TIMEOUT exists for: a worker that
        // accepts the connection and then never speaks (simulating a
        // hung podman/command, same technique as src/ssh.rs's own
        // CONNECT_TIMEOUT test) must not block ssh::exec_once's caller
        // forever. Two jobs are pointed at the *same* hung listener
        // (both never get a response) specifically to prove the bound
        // applies per-job, independently -- the pass still finishes in
        // roughly `2 * ssh_timeout`, not hanging on the first one and
        // never reaching the second.
        let pool = crate::db::init_pool(":memory:").await.unwrap();

        let hung_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let hung_port = hung_listener.local_addr().unwrap().port();

        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'hung-job-one', 'default', '{}', 'ContainerRunning', '127.0.0.1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j2', 'hung-job-two', 'default', '{}', 'ContainerRunning', '127.0.0.1', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port: hung_port,
        };

        // Outer timeout is a test-safety net, not the thing under test:
        // if poll_once_with_timeout itself failed to bound anything,
        // this would hang until it fires and the test would fail with a
        // clear message instead of hanging the whole suite.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            poll_once_with_timeout(&pool, &ssh_config, Duration::from_millis(200)),
        )
        .await;

        assert!(
            result.is_ok(),
            "poll_once_with_timeout did not return within 5s -- a hung worker blocked the whole pass"
        );
        assert_eq!(result.unwrap().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_poll_once_prunes_samples_for_jobs_no_longer_container_running() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        // j1: a stale sample for a job that has since moved on.
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'Succeeded', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, cpu_millicores, memory_usage_bytes, \
             node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
             VALUES ('j1', 100, 1000, 2000, 1000, 1, 0.1, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: String::new(),
            port: 1,
        };
        poll_once(&pool, &ssh_config).await.unwrap();

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worker_metrics WHERE job_id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_forget_stale_samples_keeps_a_pre_container_running_sample() {
        // The other half of the test above: a sample for a job still
        // mid-provisioning (VolumeAttached, pre-ContainerRunning) must
        // survive pruning, not just a ContainerRunning one -- proving
        // the prune condition widened in lockstep with the collection
        // window, not just the SELECT in poll_once_with_timeout.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VolumeAttached', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, cpu_millicores, memory_usage_bytes, \
             node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
             VALUES ('j1', NULL, NULL, 2000, 1000, 1, 0.1, 0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        forget_stale_samples(&pool).await.unwrap();

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worker_metrics WHERE job_id = 'j1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }
}
