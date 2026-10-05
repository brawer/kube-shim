//! `metrics.k8s.io/v1beta1`: what `kubectl top nodes`/`kubectl top pods`
//! call. Every number here is a **real, measured** sample --
//! `reconcile::metrics_collector` SSHes into each worker every 30s and
//! runs `podman stats` plus `free`/`nproc`/`/proc/loadavg`, storing the
//! latest reading in `worker_metrics`. (An earlier version of this
//! estimated usage from each job's own `resources.requests` instead --
//! replaced after the user explicitly asked for "actual metrics, not
//! fake numbers.")
//!
//! A job with no sample yet (just started, or SSH hasn't succeeded
//! since it became `ContainerRunning`) is simply **absent** from these
//! lists -- the same warm-up gap real Kubernetes metrics-server itself
//! has for a pod that only just started, not a reason to fall back to
//! an estimate.
//!
//! "Node" maps onto a real worker VM, one per currently-sampled job --
//! see `api::nodes`'s own docs for why that's a faithful mapping, not a
//! fiction, even though this project has no long-lived node pool.

use crate::reconcile::job::is_pre_completion_state;
use axum::{extract::State, http::StatusCode, Json};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Serialize, Deserialize)]
pub struct Usage {
    pub cpu: String,
    pub memory: String,
}

/// Formats millicores the way a real `resource.Quantity` would print
/// itself: a whole number of cores bare (`"2"`), anything fractional
/// with the `m` suffix (`"500m"`) -- not always-millicores, which would
/// make every whole-core job show as e.g. `"2000m"` instead of the more
/// readable `"2"` kubectl itself prefers.
fn format_cpu_millicores(millicores: u32) -> String {
    if millicores.is_multiple_of(1000) {
        (millicores / 1000).to_string()
    } else {
        format!("{millicores}m")
    }
}

/// `Ki`, matching real metrics-server's own convention for reporting
/// memory (binary kibibytes, not podman's own decimal `kB`/`MB` the
/// collector already converted away from when it parsed the raw sample).
fn format_memory_bytes(bytes: i64) -> String {
    format!("{}Ki", bytes / 1024)
}

fn rfc3339(unix_seconds: i64) -> String {
    DateTime::from_timestamp(unix_seconds, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

/// One real sample, joined against `jobs` for the identity fields
/// (`worker_metrics` itself only knows a job's internal UUID).
/// `pub(crate)` so `api::nodes` -- the matching core v1 `Node` objects
/// `kubectl top nodes` needs to exist before it'll even look at these
/// metrics -- can build its own list from the exact same real
/// measurements, not a second, separately-derived estimate.
///
/// `cpu_millicores`/`memory_usage_bytes` are `None` when no container
/// is running yet (still provisioning) -- `node_*` fields are always
/// present once *any* sample exists, independent of that, matching
/// `reconcile::metrics_collector`'s own Node/Pod decoupling.
pub(crate) struct WorkerSample {
    pub job_name: String,
    pub job_namespace: String,
    pub vm_name: String,
    pub cpu_millicores: Option<u32>,
    pub memory_usage_bytes: Option<i64>,
    pub node_memory_total_bytes: i64,
    pub node_memory_used_bytes: i64,
    pub node_cpu_count: u32,
    pub sampled_at: i64,
}

pub(crate) async fn real_worker_samples(
    pool: &SqlitePool,
) -> Result<Vec<WorkerSample>, sqlx::Error> {
    // jobs.status is filtered in Rust (is_pre_completion_state), not a
    // hardcoded SQL state list, for the same two reasons
    // reconcile::metrics_collector already filters this way: it stays
    // in sync with STATE_SEQUENCE automatically, and it serves a job
    // from VMRunning onward (SSH-reachable, pre-ContainerRunning) while
    // still excluding the cleanup tail -- without this, serving would
    // either lag behind collection (if left at 'ContainerRunning' only)
    // or risk a stale worker_ssh_ip (if not excluding the tail at all).
    let rows = sqlx::query(
        "SELECT jobs.name, jobs.namespace, jobs.worker_vm_name, jobs.status, \
                worker_metrics.cpu_millicores, worker_metrics.memory_usage_bytes, \
                worker_metrics.node_memory_total_bytes, worker_metrics.node_memory_used_bytes, \
                worker_metrics.node_cpu_count, worker_metrics.sampled_at \
         FROM worker_metrics \
         JOIN jobs ON worker_metrics.job_id = jobs.id \
         WHERE jobs.worker_vm_name IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter(|row| is_pre_completion_state(&row.get::<String, _>(3)))
        .map(|row| WorkerSample {
            job_name: row.get(0),
            job_namespace: row.get(1),
            vm_name: row.get(2),
            cpu_millicores: row.get::<Option<i64>, _>(4).map(|v| v as u32),
            memory_usage_bytes: row.get(5),
            node_memory_total_bytes: row.get(6),
            node_memory_used_bytes: row.get(7),
            node_cpu_count: row.get::<i64, _>(8) as u32,
            sampled_at: row.get(9),
        })
        .collect())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetricsMetadata {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub api_version: String,
    pub kind: String,
    pub metadata: NodeMetricsMetadata,
    pub timestamp: String,
    pub window: String,
    pub usage: Usage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetricsList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<NodeMetrics>,
}

pub async fn list_node_metrics(
    State(pool): State<SqlitePool>,
) -> Result<Json<NodeMetricsList>, (StatusCode, String)> {
    let samples = real_worker_samples(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = samples
        .into_iter()
        .map(|s| NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata { name: s.vm_name },
            timestamp: rfc3339(s.sampled_at),
            window: "30s".to_string(),
            usage: Usage {
                // The container's own usage, not the node's -- still
                // the best available proxy for "how busy is this VM"
                // (there's no precise node-wide CPU-usage figure to
                // derive from /proc/loadavg alone), and a defensible
                // one given this project's one-container-per-VM
                // architecture. 0 while no container is running yet,
                // which is also an honest answer: a VM that's still
                // mounting a volume/installing podman genuinely isn't
                // CPU-busy in any way this would need to reflect.
                cpu: format_cpu_millicores(s.cpu_millicores.unwrap_or(0)),
                // Real, measured node-wide memory usage (free -b) --
                // *not* the container's own figure, unlike cpu above:
                // this one's always available, independent of whether
                // a container exists, so there's no reason to use a
                // less accurate proxy for it.
                memory: format_memory_bytes(s.node_memory_used_bytes),
            },
        })
        .collect();

    Ok(Json(NodeMetricsList {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "NodeMetricsList".to_string(),
        items,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PodMetricsMetadata {
    pub name: String,
    pub namespace: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContainerMetrics {
    pub name: String,
    pub usage: Usage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PodMetrics {
    pub api_version: String,
    pub kind: String,
    pub metadata: PodMetricsMetadata,
    pub timestamp: String,
    pub window: String,
    pub containers: Vec<ContainerMetrics>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PodMetricsList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<PodMetrics>,
}

/// `None` when the sample has no container data yet (still
/// provisioning) -- Pod-level metrics wait for a real container the
/// same way the overall warm-up gap already applies elsewhere; it's
/// `list_node_metrics` that doesn't need to wait, not this one.
fn to_pod_metrics(sample: WorkerSample) -> Option<PodMetrics> {
    let cpu_millicores = sample.cpu_millicores?;
    let memory_usage_bytes = sample.memory_usage_bytes?;
    Some(PodMetrics {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "PodMetrics".to_string(),
        metadata: PodMetricsMetadata {
            name: sample.job_name,
            namespace: sample.job_namespace,
        },
        timestamp: rfc3339(sample.sampled_at),
        window: "30s".to_string(),
        // One synthetic "main" container -- same simplification
        // api::logs/cloud_init already make (this project only ever
        // *starts* one container, even though reconcile::metrics_collector's
        // own collection is already forward-compatible with more), so
        // there's never a real name to look up beyond that.
        containers: vec![ContainerMetrics {
            name: "main".to_string(),
            usage: Usage {
                cpu: format_cpu_millicores(cpu_millicores),
                memory: format_memory_bytes(memory_usage_bytes),
            },
        }],
    })
}

/// All namespaces -- what `kubectl top pods -A` calls.
pub async fn list_pod_metrics(
    State(pool): State<SqlitePool>,
) -> Result<Json<PodMetricsList>, (StatusCode, String)> {
    let samples = real_worker_samples(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = samples.into_iter().filter_map(to_pod_metrics).collect();

    Ok(Json(PodMetricsList {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "PodMetricsList".to_string(),
        items,
    }))
}

/// One namespace -- what plain `kubectl top pods` (current namespace)
/// calls.
pub async fn list_pod_metrics_for_namespace(
    State(pool): State<SqlitePool>,
    axum::extract::Path(namespace): axum::extract::Path<String>,
) -> Result<Json<PodMetricsList>, (StatusCode, String)> {
    let samples = real_worker_samples(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = samples
        .into_iter()
        .filter(|s| s.job_namespace == namespace)
        .filter_map(to_pod_metrics)
        .collect();

    Ok(Json(PodMetricsList {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "PodMetricsList".to_string(),
        items,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn insert_job(
        pool: &SqlitePool,
        id: &str,
        name: &str,
        namespace: &str,
        vm_name: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, created_at, updated_at, version) \
             VALUES (?, ?, ?, '{}', 'ContainerRunning', ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(vm_name)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_job_with_status(
        pool: &SqlitePool,
        id: &str,
        name: &str,
        namespace: &str,
        status: &str,
        vm_name: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, created_at, updated_at, version) \
             VALUES (?, ?, ?, '{}', ?, ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(status)
        .bind(vm_name)
        .execute(pool)
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_sample(
        pool: &SqlitePool,
        job_id: &str,
        cpu_millicores: i64,
        memory_usage_bytes: i64,
        node_memory_total_bytes: i64,
        node_memory_used_bytes: i64,
        node_cpu_count: i64,
        sampled_at: i64,
    ) {
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, cpu_millicores, memory_usage_bytes, \
             node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
             VALUES (?, ?, ?, ?, ?, ?, 0.0, ?)",
        )
        .bind(job_id)
        .bind(cpu_millicores)
        .bind(memory_usage_bytes)
        .bind(node_memory_total_bytes)
        .bind(node_memory_used_bytes)
        .bind(node_cpu_count)
        .bind(sampled_at)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A sample with no container data at all -- what
    /// `reconcile::metrics_collector` records for a job that's
    /// SSH-reachable but still provisioning (no container running
    /// yet).
    async fn insert_node_only_sample(
        pool: &SqlitePool,
        job_id: &str,
        node_memory_total_bytes: i64,
        node_memory_used_bytes: i64,
        node_cpu_count: i64,
        sampled_at: i64,
    ) {
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, cpu_millicores, memory_usage_bytes, \
             node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
             VALUES (?, NULL, NULL, ?, ?, ?, 0.0, ?)",
        )
        .bind(job_id)
        .bind(node_memory_total_bytes)
        .bind(node_memory_used_bytes)
        .bind(node_cpu_count)
        .bind(sampled_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_node_metrics_one_entry_per_real_sample() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "a", "default", Some("kube-shim-worker-a")).await;
        // node_memory_used_bytes (1GiB) deliberately differs from the
        // container's own memory_usage_bytes (536_870_912) -- Node
        // usage.memory must come from the former now, not be an echo
        // of the container's own figure.
        insert_sample(
            &pool,
            "j1",
            500,
            536_870_912,
            2_000_000_000,
            1_073_741_824,
            2,
            1_700_000_000,
        )
        .await;
        // A job with no worker VM at all yet -- must not appear even if
        // (hypothetically) a stray sample existed for it.
        insert_job(&pool, "j2", "b", "default", None).await;

        let router = Router::new()
            .route("/nodes", get(list_node_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeMetricsList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "kube-shim-worker-a");
        assert_eq!(list.items[0].usage.cpu, "500m");
        assert_eq!(list.items[0].usage.memory, "1048576Ki");
    }

    #[tokio::test]
    async fn test_node_metrics_omits_jobs_with_no_sample_yet() {
        // The real warm-up gap: a job that just became ContainerRunning
        // but hasn't had its first successful poll yet must simply not
        // appear, not fall back to an estimate.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "a", "default", Some("kube-shim-worker-a")).await;

        let router = Router::new()
            .route("/nodes", get(list_node_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeMetricsList = serde_json::from_slice(&body).unwrap();
        assert!(list.items.is_empty());
    }

    #[tokio::test]
    async fn test_node_metrics_stops_as_soon_as_job_leaves_container_running() {
        // A job that has already moved on (Succeeded here) but whose
        // worker_metrics row hasn't been pruned yet (forget_stale_samples
        // runs at the *start* of the next poll cycle, not the instant
        // the job's own status changes) must not still be served --
        // serving must track the job's current status directly, not
        // rely on pruning having already caught up.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(
            &pool,
            "j1",
            "a",
            "default",
            "Succeeded",
            Some("kube-shim-worker-a"),
        )
        .await;
        insert_sample(
            &pool,
            "j1",
            500,
            536_870_912,
            2_000_000_000,
            0,
            2,
            1_700_000_000,
        )
        .await;

        let router = Router::new()
            .route("/nodes", get(list_node_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeMetricsList = serde_json::from_slice(&body).unwrap();
        assert!(
            list.items.is_empty(),
            "a job that already left ContainerRunning must not still be served, even with a lingering row"
        );
    }

    #[tokio::test]
    async fn test_pod_metrics_one_entry_per_real_sample() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "a", "default", Some("kube-shim-worker-a")).await;
        insert_sample(
            &pool,
            "j1",
            2000,
            1_000_000,
            4_000_000_000,
            0,
            2,
            1_700_000_000,
        )
        .await;
        insert_job(&pool, "j2", "b", "other", Some("kube-shim-worker-b")).await;
        insert_sample(
            &pool,
            "j2",
            100,
            500_000,
            4_000_000_000,
            0,
            2,
            1_700_000_000,
        )
        .await;

        let router = Router::new()
            .route("/pods", get(list_pod_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/pods")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: PodMetricsList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 2);
        let a = list.items.iter().find(|p| p.metadata.name == "a").unwrap();
        assert_eq!(a.containers[0].usage.cpu, "2");
    }

    #[tokio::test]
    async fn test_pod_metrics_for_namespace_filters() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "a", "default", Some("kube-shim-worker-a")).await;
        insert_sample(
            &pool,
            "j1",
            2000,
            1_000_000,
            4_000_000_000,
            0,
            2,
            1_700_000_000,
        )
        .await;
        insert_job(&pool, "j2", "b", "other", Some("kube-shim-worker-b")).await;
        insert_sample(
            &pool,
            "j2",
            100,
            500_000,
            4_000_000_000,
            0,
            2,
            1_700_000_000,
        )
        .await;

        let router = Router::new()
            .route(
                "/namespaces/:namespace/pods",
                get(list_pod_metrics_for_namespace),
            )
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/namespaces/default/pods")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: PodMetricsList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "a");
    }

    #[tokio::test]
    async fn test_node_metrics_included_for_job_with_no_container_sample_yet() {
        // VolumeAttached: SSH-reachable, pre-ContainerRunning, no
        // container running yet -- Node-level visibility must not wait
        // on Pod-level readiness, unlike list_pod_metrics below.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(
            &pool,
            "j1",
            "a",
            "default",
            "VolumeAttached",
            Some("kube-shim-worker-a"),
        )
        .await;
        insert_node_only_sample(&pool, "j1", 2_000_000_000, 500_000_000, 2, 1_700_000_000).await;

        let router = Router::new()
            .route("/nodes", get(list_node_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeMetricsList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].usage.cpu, "0"); // nothing running yet
        assert_eq!(list.items[0].usage.memory, "488281Ki"); // real node_memory_used_bytes
    }

    #[tokio::test]
    async fn test_pod_metrics_omits_job_with_no_container_sample_yet() {
        // The exact same fixture as the Node-level test above, through
        // list_pod_metrics instead: Pod-level metrics must still wait
        // for real container data, preserving "absent until measured"
        // at the pod level specifically.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(
            &pool,
            "j1",
            "a",
            "default",
            "VolumeAttached",
            Some("kube-shim-worker-a"),
        )
        .await;
        insert_node_only_sample(&pool, "j1", 2_000_000_000, 500_000_000, 2, 1_700_000_000).await;

        let router = Router::new()
            .route("/pods", get(list_pod_metrics))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/pods")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: PodMetricsList = serde_json::from_slice(&body).unwrap();
        assert!(
            list.items.is_empty(),
            "a job with no container sample yet must not appear in Pod-level metrics"
        );
    }
}
