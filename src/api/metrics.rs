//! `metrics.k8s.io/v1beta1` (Phase 12): what `kubectl top nodes`/`kubectl
//! top pods` call. There's no real metrics-collection agent anywhere in
//! this project (no cAdvisor, no actual per-container CPU/memory
//! sampling over SSH) -- every number here is an *estimate* straight
//! from each job's own `resources.requests`, the same values
//! `reconcile::job::handle_vm_pending` already uses to size the worker
//! VM itself. That's a deliberate simplification matching the plan's own
//! wording ("Estimate CPU/memory from running jobs"), not a stand-in for
//! a feature that was supposed to measure something real.
//!
//! "Node" is *not* a fiction here, on reflection -- a first version of
//! this reported one synthetic aggregate node summing every job's
//! request, reasoning that this project has no node pool at all. But a
//! worker VM genuinely *is* a node in the real Kubernetes sense: a
//! machine that runs exactly one pod. It just doesn't live in a
//! long-lived static pool the way a real cluster's nodes do -- it's
//! created and destroyed together with the one job it exists for. One
//! `NodeMetrics` entry per currently-allocated worker VM (`api::nodes`'s
//! own `Node` objects use the same set) is a faithful mapping onto that,
//! not a fiction layered on top of it.

use crate::reconcile::job::extract_resource_requests;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
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

fn format_usage(cpu_millicores: u32, memory_mb: u32) -> Usage {
    Usage {
        cpu: format_cpu_millicores(cpu_millicores),
        memory: format!("{memory_mb}Mi"),
    }
}

/// Every non-terminal job's own `(cpu_millicores, memory_mb)` request,
/// defaulting the same way `handle_vm_pending` does when a value is
/// missing (1 core / 1GB, i.e. 1000 millicores / 1024 MB) -- full
/// request precision, not rounded up to whatever got provisioned (see
/// `reconcile::job::extract_resource_requests`'s own docs on why).
async fn non_terminal_job_requests(
    pool: &SqlitePool,
) -> Result<Vec<(String, String, u32, u32)>, sqlx::Error> {
    let rows = sqlx::query("SELECT name, namespace, spec FROM jobs WHERE status != 'Archived'")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let name: String = row.get(0);
            let namespace: String = row.get(1);
            let spec_str: String = row.get(2);
            let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
            let (cpu_millicores, memory_mb) = extract_resource_requests(&spec);
            (
                name,
                namespace,
                cpu_millicores.unwrap_or(1000),
                memory_mb.unwrap_or(1024),
            )
        })
        .collect())
}

/// One entry per currently-allocated worker VM -- `pub(crate)` so
/// `api::nodes` (the matching core v1 `Node` objects `kubectl top
/// nodes` needs to exist before it'll even look at these metrics) can
/// build its list from the exact same set, by name. A job only gets an
/// entry once `handle_vm_pending` has actually recorded a
/// `worker_vm_name` for it -- no VM yet means nothing to report as a
/// node yet, same as a real machine that hasn't booted.
pub(crate) struct AllocatedWorkerVm {
    pub vm_name: String,
    pub request_cpu_millicores: u32,
    pub request_memory_mb: u32,
}

pub(crate) async fn allocated_worker_vms(
    pool: &SqlitePool,
) -> Result<Vec<AllocatedWorkerVm>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT worker_vm_name, spec FROM jobs \
         WHERE status != 'Archived' AND worker_vm_name IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let vm_name: String = row.get(0);
            let spec_str: String = row.get(1);
            let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
            let (cpu_millicores, memory_mb) = extract_resource_requests(&spec);
            AllocatedWorkerVm {
                vm_name,
                request_cpu_millicores: cpu_millicores.unwrap_or(1000),
                request_memory_mb: memory_mb.unwrap_or(1024),
            }
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
    let vms = allocated_worker_vms(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = vms
        .into_iter()
        .map(|vm| NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata { name: vm.vm_name },
            timestamp: chrono::Utc::now().to_rfc3339(),
            window: "10s".to_string(),
            usage: format_usage(vm.request_cpu_millicores, vm.request_memory_mb),
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

fn to_pod_metrics(name: String, namespace: String, cpu_cores: u32, memory_gb: u32) -> PodMetrics {
    PodMetrics {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "PodMetrics".to_string(),
        metadata: PodMetricsMetadata { name, namespace },
        timestamp: chrono::Utc::now().to_rfc3339(),
        window: "10s".to_string(),
        // One synthetic "main" container -- same simplification
        // api::logs/cloud_init already make (this project only ever
        // supports a job's first container), so there's never a real
        // name to look up beyond that.
        containers: vec![ContainerMetrics {
            name: "main".to_string(),
            usage: format_usage(cpu_cores, memory_gb),
        }],
    }
}

/// All namespaces -- what `kubectl top pods -A` calls.
pub async fn list_pod_metrics(
    State(pool): State<SqlitePool>,
) -> Result<Json<PodMetricsList>, (StatusCode, String)> {
    let requests = non_terminal_job_requests(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = requests
        .into_iter()
        .map(|(name, namespace, cpu, mem)| to_pod_metrics(name, namespace, cpu, mem))
        .collect();

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
    let requests = non_terminal_job_requests(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = requests
        .into_iter()
        .filter(|(_, ns, _, _)| *ns == namespace)
        .map(|(name, namespace, cpu, mem)| to_pod_metrics(name, namespace, cpu, mem))
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
        status: &str,
        spec: &str,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES (?, ?, ?, ?, ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(spec)
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_job_with_vm(
        pool: &SqlitePool,
        id: &str,
        name: &str,
        namespace: &str,
        status: &str,
        spec: &str,
        vm_name: &str,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, created_at, updated_at, version) \
             VALUES (?, ?, ?, ?, ?, ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(spec)
        .bind(status)
        .bind(vm_name)
        .execute(pool)
        .await
        .unwrap();
    }

    fn spec_with_requests(cpu: &str, memory: &str) -> String {
        serde_json::json!({
            "template": {"spec": {"containers": [{
                "name": "x", "image": "y",
                "resources": {"requests": {"cpu": cpu, "memory": memory}}
            }]}}
        })
        .to_string()
    }

    #[tokio::test]
    async fn test_node_metrics_one_entry_per_allocated_worker_vm() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(
            &pool,
            "j1",
            "a",
            "default",
            "VMRunning",
            &spec_with_requests("2", "4Gi"),
            "kube-shim-worker-a",
        )
        .await;
        // No worker VM yet -- must not get a node entry.
        insert_job(
            &pool,
            "j2",
            "b",
            "default",
            "VolumePending",
            &spec_with_requests("1", "2Gi"),
        )
        .await;
        // Archived -- must not count even if it still has a vm name.
        insert_job_with_vm(
            &pool,
            "j3",
            "c",
            "default",
            "Archived",
            &spec_with_requests("8", "16Gi"),
            "kube-shim-worker-c",
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
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "kube-shim-worker-a");
        assert_eq!(list.items[0].usage.cpu, "2");
        assert_eq!(list.items[0].usage.memory, "4096Mi");
    }

    #[tokio::test]
    async fn test_node_metrics_defaults_missing_requests_to_one_and_one() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(
            &pool,
            "j1",
            "a",
            "default",
            "VMRunning",
            "{}",
            "kube-shim-worker-a",
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
        assert_eq!(list.items[0].usage.cpu, "1");
        assert_eq!(list.items[0].usage.memory, "1024Mi");
    }

    #[tokio::test]
    async fn test_node_metrics_preserves_fractional_cpu_and_sub_gigabyte_memory() {
        // The whole reason request_cpu_millicores/request_memory_mb
        // exist instead of reusing handle_vm_pending's own whole-unit
        // rounding: a job that asked for "500m"/"512Mi" must be
        // reported as exactly that, not overstated to "1"/"1Gi" just
        // because that's what the worker VM actually got provisioned
        // as underneath it.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(
            &pool,
            "j1",
            "a",
            "default",
            "VMRunning",
            &spec_with_requests("500m", "512Mi"),
            "kube-shim-worker-a",
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
        assert_eq!(list.items[0].usage.cpu, "500m");
        assert_eq!(list.items[0].usage.memory, "512Mi");
    }

    #[tokio::test]
    async fn test_pod_metrics_one_entry_per_non_terminal_job() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(
            &pool,
            "j1",
            "a",
            "default",
            "VMRunning",
            &spec_with_requests("2", "4Gi"),
        )
        .await;
        insert_job(
            &pool,
            "j2",
            "b",
            "other",
            "VolumePending",
            &spec_with_requests("1", "2Gi"),
        )
        .await;
        insert_job(
            &pool,
            "j3",
            "c",
            "default",
            "Archived",
            &spec_with_requests("8", "16Gi"),
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
        insert_job(
            &pool,
            "j1",
            "a",
            "default",
            "VMRunning",
            &spec_with_requests("2", "4Gi"),
        )
        .await;
        insert_job(
            &pool,
            "j2",
            "b",
            "other",
            "VolumePending",
            &spec_with_requests("1", "2Gi"),
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
}
