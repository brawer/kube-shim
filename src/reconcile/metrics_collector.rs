//! Real worker-VM metrics, collected over SSH -- replaces `api::metrics`'
//! original design, which only ever estimated usage from a job's own
//! `resources.requests` (the plan's own capacity table, not anything
//! actually measured). "I'd like actual metrics, not fake numbers" --
//! this is that.
//!
//! One combined SSH round-trip per currently-`ContainerRunning` job,
//! every `reconcile::METRICS_POLL_INTERVAL`: `podman stats --format
//! json` for the job's own container (verified hands-on against both a
//! local podman 6.1.2 and the real apt-installed podman 4.9.3 this
//! project's own worker VMs actually run -- same JSON schema on both),
//! plus `free -b`/`nproc`/`/proc/loadavg` for the machine as a whole.
//! Same SSH-safety rule `api::logs` already established: only ever
//! attempted while `status == "ContainerRunning"` (a stale
//! `worker_ssh_ip` could otherwise point at a since-reassigned IP, and
//! this module's host-key verification is the same "accept any key" as
//! everywhere else SSH is used here -- see `src/ssh.rs`'s own docs).
//!
//! Deliberately "latest sample, upserted" (`worker_metrics`), not a
//! time series -- real Kubernetes metrics-server itself only ever
//! serves the most recent window too, never history.

use crate::ssh::{self, WorkerSshConfig};
use anyhow::Result;
use chrono::Utc;
use sqlx::{Row, SqlitePool};

/// One shell round-trip: the job's own container's stats, then the
/// machine's overall memory/CPU/load, separated by a marker that can't
/// collide with any of their own real output.
const SEP: &str = "___KUBESHIM_METRICS_SEP___";

fn collection_command() -> String {
    format!(
        "podman stats --no-stream --format json \"$(cat /tmp/container-id.txt 2>/dev/null)\" 2>/dev/null; \
         echo '{SEP}'; \
         free -b | awk '/^Mem:/ {{print $2, $3}}'; \
         echo '{SEP}'; \
         nproc; \
         echo '{SEP}'; \
         cat /proc/loadavg"
    )
}

#[derive(Debug, PartialEq)]
struct Sample {
    cpu_millicores: u32,
    memory_usage_bytes: u64,
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

/// `cpu_percent`/`mem_usage` from `podman stats --format json`'s first
/// (and only, since we always ask for exactly one container) array
/// entry -- e.g. `{"cpu_percent": "0.26%", "mem_usage": "303.1kB / 8.289GB", ...}`.
/// `cpu_percent` is already relative to one full core (podman/docker's
/// own long-standing convention: `"100%"` means one core fully
/// saturated, not "100% of all cores"), so converting to millicores is
/// a direct `* 10`, not a core-count-dependent calculation.
fn parse_podman_stats(json: &str) -> Option<(u32, u64)> {
    let parsed: serde_json::Value = serde_json::from_str(json).ok()?;
    let entry = parsed.as_array()?.first()?;
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
    Some((cpu_millicores, memory_usage_bytes))
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

fn parse_sample(stdout: &str) -> Option<Sample> {
    let mut sections = stdout.split(SEP);
    let stats_section = sections.next()?.trim();
    let free_section = sections.next()?.trim();
    let nproc_section = sections.next()?.trim();
    let loadavg_section = sections.next()?.trim();

    let (cpu_millicores, memory_usage_bytes) = parse_podman_stats(stats_section)?;
    let (node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1) =
        parse_node_health(free_section, nproc_section, loadavg_section)?;

    Some(Sample {
        cpu_millicores,
        memory_usage_bytes,
        node_memory_total_bytes,
        node_memory_used_bytes,
        node_cpu_count,
        node_load1,
    })
}

async fn upsert_sample(pool: &SqlitePool, job_id: &str, sample: &Sample) -> Result<()> {
    let now = Utc::now().timestamp();
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
    .bind(sample.cpu_millicores)
    .bind(sample.memory_usage_bytes as i64)
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
    sqlx::query(
        "DELETE FROM worker_metrics WHERE job_id NOT IN \
         (SELECT id FROM jobs WHERE status = 'ContainerRunning')",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// One polling pass: every job currently `ContainerRunning` with a
/// known `worker_ssh_ip` gets one SSH round-trip; a job whose SSH call
/// fails (worker not reachable *yet*, e.g. firewall rules only just
/// applied) or whose output doesn't parse just keeps its last known
/// sample (if any) rather than being zeroed out or removed -- a single
/// missed poll isn't a reason to make `kubectl top` go blank for 30s.
/// Returns how many samples were successfully updated.
pub async fn poll_once(pool: &SqlitePool, ssh_config: &WorkerSshConfig) -> Result<usize> {
    forget_stale_samples(pool).await?;

    let rows = sqlx::query(
        "SELECT id, worker_ssh_ip FROM jobs WHERE status = 'ContainerRunning' AND worker_ssh_ip IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    let command = collection_command();
    let mut updated = 0;
    for row in rows {
        let job_id: String = row.get(0);
        let ip: String = row.get(1);

        let output =
            match ssh::exec_once(&ip, ssh_config.port, &ssh_config.private_key, &command).await {
                Ok(output) => output,
                Err(err) => {
                    tracing::debug!(
                        "metrics poll: SSH to {ip} for job {job_id} failed, will retry: {err}"
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
        let (cpu_millicores, memory_usage_bytes) = parse_podman_stats(json).unwrap();
        assert_eq!(cpu_millicores, 1); // 0.12% * 10, rounded
        assert_eq!(memory_usage_bytes, 294_900);
    }

    #[test]
    fn test_parse_podman_stats_empty_array_is_none() {
        // What a container that no longer exists (or never started)
        // produces: podman succeeds but reports nothing.
        assert!(parse_podman_stats("[]").is_none());
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
        let stats = r#"[{"cpu_percent": "25.00%", "mem_usage": "512.0MB / 1.0GB"}]"#;
        let stdout = format!(
            "{stats}\n{SEP}\n8000000000 4000000000\n{SEP}\n4\n{SEP}\n1.00 0.90 0.80 1/200 999\n"
        );
        let sample = parse_sample(&stdout).unwrap();
        assert_eq!(sample.cpu_millicores, 250);
        assert_eq!(sample.memory_usage_bytes, 512_000_000);
        assert_eq!(sample.node_memory_total_bytes, 8_000_000_000);
        assert_eq!(sample.node_memory_used_bytes, 4_000_000_000);
        assert_eq!(sample.node_cpu_count, 4);
        assert_eq!(sample.node_load1, 1.0);
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
        assert_eq!(sample.cpu_millicores, 0); // 0.02% rounds down to 0m
        assert_eq!(sample.memory_usage_bytes, 1_839_000);
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
    async fn test_poll_once_ignores_jobs_not_container_running() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'job-one', 'default', '{}', 'VMRunning', '127.0.0.1', 0, 0, 1)",
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
}
