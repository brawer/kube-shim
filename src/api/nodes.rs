//! Minimal synthetic core v1 Node list (Phase 12 follow-up) -- `kubectl
//! top nodes` needs a real Node object to correlate against
//! `metrics.k8s.io`'s own `NodeMetrics` by name, not just the metrics
//! themselves (verified hands-on: without this, `kubectl top nodes`
//! calls `GET /api/v1/nodes`, gets a 404, and gives up before even
//! trying the metrics endpoint). See `api::metrics`'s own docs for why
//! reporting one entry per currently-allocated worker VM is the honest
//! mapping here, not a fiction: a worker VM genuinely is a node in the
//! real Kubernetes sense, a machine running exactly one pod, for
//! exactly as long as it and its one job exist.

use super::metrics::allocated_worker_vms;
use crate::workload::smallest_fitting_server_plan;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use sqlx::SqlitePool;

#[derive(Debug, Serialize)]
pub struct NodeMetadata {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct NodeResources {
    pub cpu: String,
    pub memory: String,
}

#[derive(Debug, Serialize)]
pub struct NodeCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct NodeStatus {
    /// The worker VM's actual provisioned plan size (whatever
    /// `smallest_fitting_server_plan` rounded the job's own request up
    /// to -- the real capacity UpCloud is billing for), not the raw
    /// request itself. `allocatable` is the same number: there's no
    /// system-reserved overhead to subtract, since nothing but the one
    /// job's own container ever runs on this VM.
    pub capacity: NodeResources,
    pub allocatable: NodeResources,
    pub conditions: Vec<NodeCondition>,
}

#[derive(Debug, Serialize)]
pub struct Node {
    pub api_version: String,
    pub kind: String,
    pub metadata: NodeMetadata,
    pub status: NodeStatus,
}

#[derive(Debug, Serialize)]
pub struct NodeList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Node>,
}

pub async fn list_nodes(
    State(pool): State<SqlitePool>,
) -> Result<Json<NodeList>, (StatusCode, String)> {
    let vms = allocated_worker_vms(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = vms
        .into_iter()
        .map(|vm| {
            // Same rounding-up-to-whole-units `handle_vm_pending` itself
            // does before calling this -- the node's reported capacity
            // is what UpCloud actually provisioned, which is always a
            // whole core/GB plan, never the job's own (possibly
            // fractional) request.
            let cpu_cores = vm.request_cpu_millicores.div_ceil(1000);
            let memory_gb = vm.request_memory_mb.div_ceil(1024);
            let plan = smallest_fitting_server_plan(cpu_cores, memory_gb);
            let (cpu, memory) = plan
                .map(|p| (p.cpu_cores, p.memory_gb))
                .unwrap_or((cpu_cores, memory_gb));
            let resources = NodeResources {
                cpu: cpu.to_string(),
                memory: format!("{memory}Gi"),
            };
            Node {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                metadata: NodeMetadata { name: vm.vm_name },
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

    async fn insert_job_with_vm(
        pool: &SqlitePool,
        id: &str,
        status: &str,
        spec: &str,
        vm_name: &str,
    ) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_vm_name, created_at, updated_at, version) \
             VALUES (?, ?, 'default', ?, ?, ?, 0, 0, 1)",
        )
        .bind(id)
        .bind(id)
        .bind(spec)
        .bind(status)
        .bind(vm_name)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_list_nodes_one_per_allocated_worker_vm_with_rounded_up_capacity() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let spec = serde_json::json!({
            "template": {"spec": {"containers": [{
                "name": "x", "image": "y",
                // 3 CPU / 5GB doesn't exactly match any plan -- the node's
                // capacity should reflect the plan it actually got
                // rounded up to, not the raw request.
                "resources": {"requests": {"cpu": "3", "memory": "5Gi"}}
            }]}}
        })
        .to_string();
        insert_job_with_vm(&pool, "j1", "VMRunning", &spec, "kube-shim-worker-a").await;

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
        let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(list["items"].as_array().unwrap().len(), 1);
        let node = &list["items"][0];
        assert_eq!(node["metadata"]["name"], "kube-shim-worker-a");
        // smallest_fitting_server_plan(3, 5) rounds up past the raw
        // request -- real capacity, not an echo of the ask.
        let cpu: u32 = node["status"]["capacity"]["cpu"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(cpu >= 3);
        assert_eq!(node["status"]["conditions"][0]["type"], "Ready");
        assert_eq!(node["status"]["conditions"][0]["status"], "True");
    }

    #[tokio::test]
    async fn test_list_nodes_empty_when_no_worker_vms_allocated() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
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
        let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(list["items"].as_array().unwrap().is_empty());
    }
}
