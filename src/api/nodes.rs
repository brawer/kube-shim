//! Minimal synthetic core v1 Node list -- `kubectl top nodes` needs a
//! real Node object to correlate against `metrics.k8s.io`'s own
//! `NodeMetrics` by name, not just the metrics themselves (verified
//! hands-on: without this, `kubectl top nodes` calls `GET /api/v1/nodes`,
//! gets a 404, and gives up before even trying the metrics endpoint).
//! See `api::metrics`'s own docs for why reporting one entry per
//! currently-sampled worker VM is the honest mapping here, not a
//! fiction: a worker VM genuinely is a node in the real Kubernetes
//! sense, a machine running exactly one pod, for exactly as long as it
//! and its one job exist.
//!
//! `status.capacity`/`allocatable` are the worker's *real, measured*
//! `nproc`/`free -b` totals (`reconcile::metrics_collector`'s own
//! sample), not derived from the job's `resources.requests` or looked
//! up in `workload`'s server-plan table -- the whole point of "actual
//! metrics, not fake numbers" applies to a node's own capacity just as
//! much as to usage.

use super::metrics::real_worker_samples;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeResources {
    pub cpu: String,
    pub memory: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    /// `allocatable` is the same as `capacity`: there's no
    /// system-reserved overhead to subtract, since nothing but the
    /// one job's own container ever runs on this VM.
    pub capacity: NodeResources,
    pub allocatable: NodeResources,
    pub conditions: Vec<NodeCondition>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub api_version: String,
    pub kind: String,
    pub metadata: NodeMetadata,
    pub status: NodeStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Node>,
}

pub async fn list_nodes(
    State(pool): State<SqlitePool>,
) -> Result<Json<NodeList>, (StatusCode, String)> {
    let samples = real_worker_samples(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = samples
        .into_iter()
        .map(|s| {
            let resources = NodeResources {
                cpu: s.node_cpu_count.to_string(),
                memory: format!("{}Ki", s.node_memory_total_bytes / 1024),
            };
            Node {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                metadata: NodeMetadata { name: s.vm_name },
                status: NodeStatus {
                    allocatable: NodeResources {
                        cpu: resources.cpu.clone(),
                        memory: resources.memory.clone(),
                    },
                    capacity: resources,
                    conditions: vec![NodeCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                    }],
                },
            }
        })
        .collect();

    Ok(Json(NodeList {
        api_version: "v1".to_string(),
        kind: "NodeList".to_string(),
        items,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn insert_job_with_vm(pool: &SqlitePool, id: &str, vm_name: &str) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, created_at, updated_at, version) \
             VALUES (?, ?, 'default', '{}', 'ContainerRunning', ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(id)
        .bind(vm_name)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_sample(
        pool: &SqlitePool,
        job_id: &str,
        node_memory_total_bytes: i64,
        node_cpu_count: i64,
    ) {
        sqlx::query(
            "INSERT INTO worker_metrics (job_id, cpu_millicores, memory_usage_bytes, \
             node_memory_total_bytes, node_memory_used_bytes, node_cpu_count, node_load1, sampled_at) \
             VALUES (?, 0, 0, ?, 0, ?, 0.0, 0)",
        )
        .bind(job_id)
        .bind(node_memory_total_bytes)
        .bind(node_cpu_count)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_list_nodes_one_per_real_sample_with_real_capacity() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "j1", "kube-shim-worker-a").await;
        insert_sample(&pool, "j1", 2_147_483_648, 2).await; // 2GiB, 2 cores

        let router = Router::new()
            .route("/api/v1/nodes", get(list_nodes))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeList = serde_json::from_slice(&body).unwrap_or_else(|_| {
            panic!("response did not deserialize as NodeList");
        });
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name, "kube-shim-worker-a");
        assert_eq!(list.items[0].status.capacity.cpu, "2");
        assert_eq!(list.items[0].status.capacity.memory, "2097152Ki");
        assert_eq!(list.items[0].status.conditions[0].condition_type, "Ready");
        assert_eq!(list.items[0].status.conditions[0].status, "True");
    }

    #[tokio::test]
    async fn test_list_nodes_empty_when_no_samples_yet() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_vm(&pool, "j1", "kube-shim-worker-a").await;
        // No insert_sample call -- the job exists and has a worker VM,
        // but no real measurement has arrived yet.

        let router = Router::new()
            .route("/api/v1/nodes", get(list_nodes))
            .with_state(pool);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/nodes")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: NodeList = serde_json::from_slice(&body).unwrap();
        assert!(list.items.is_empty());
    }
}
