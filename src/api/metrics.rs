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
//! "Node" is a fiction here too: this project has no Kubernetes nodes,
//! just ephemeral worker VMs, one per job run. `list_node_metrics`
//! reports exactly one synthetic node (named after `resource_prefix`)
//! whose usage is the sum of every non-terminal job's own request --
//! "total estimated load across the whole cronjob family," matching
//! this phase's own stated goal, collapsed into the one row `kubectl top
//! nodes` expects to be able to show.

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

fn format_usage(cpu_cores: u32, memory_gb: u32) -> Usage {
    Usage {
        cpu: cpu_cores.to_string(),
        memory: format!("{memory_gb}Gi"),
    }
}

/// Every non-terminal job's own `(cpu_cores, memory_gb)` request,
/// defaulting the same way `handle_vm_pending` does when a value is
/// missing (1 core / 1GB) -- so the aggregate here matches what would
/// actually get provisioned, not a silent undercount.
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
            let (cpu_cores, memory_gb) = extract_resource_requests(&spec);
            (
                name,
                namespace,
                cpu_cores.unwrap_or(1),
                memory_gb.unwrap_or(1),
            )
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
    let requests = non_terminal_job_requests(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let total_cpu: u32 = requests.iter().map(|(_, _, cpu, _)| cpu).sum();
    let total_memory: u32 = requests.iter().map(|(_, _, _, mem)| mem).sum();

    Ok(Json(NodeMetricsList {
        api_version: "metrics.k8s.io/v1beta1".to_string(),
        kind: "NodeMetricsList".to_string(),
        items: vec![NodeMetrics {
            api_version: "metrics.k8s.io/v1beta1".to_string(),
            kind: "NodeMetrics".to_string(),
            metadata: NodeMetricsMetadata {
                name: "kube-shim".to_string(),
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            window: "10s".to_string(),
            usage: format_usage(total_cpu, total_memory),
        }],
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
    async fn test_node_metrics_sums_requests_across_non_terminal_jobs() {
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
            "default",
            "VolumePending",
            &spec_with_requests("1", "2Gi"),
        )
        .await;
        // Archived -- must not count.
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
        assert_eq!(list.items[0].usage.cpu, "3");
        assert_eq!(list.items[0].usage.memory, "6Gi");
    }

    #[tokio::test]
    async fn test_node_metrics_defaults_missing_requests_to_one_and_one() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "a", "default", "Created", "{}").await;

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
        assert_eq!(list.items[0].usage.memory, "1Gi");
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
