//! `GET .../pods/{name}/log` (Phase 10) -- `kubectl logs`/`kubectl logs
//! -f`. One pod per job run, matching this project's model everywhere
//! else (a job's own `name` *is* the pod name); there's no separate
//! `Job`/`Pod` resource hierarchy to resolve through.
//!
//! **Live SSH is only ever attempted while `status == "ContainerRunning"`,
//! deliberately.** `jobs.worker_ssh_ip` is never cleared once a job moves
//! past that state, but the worker VM it pointed at is deleted a few
//! states later (`VMTerminating`) -- and `src/ssh.rs`'s host-key
//! verification accepts *any* key, so blindly SSHing to a stale IP could
//! silently succeed against a completely unrelated machine that
//! happens to have been handed the same address later, rather than
//! erroring. Every other case falls back to `jobs.cached_logs`, written
//! once by `reconcile::job::handle_container_running` right before the
//! worker is torn down -- see that module's own docs.

use crate::k8s_status;
use crate::ssh::{self, WorkerSshConfig};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use serde::Deserialize;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct LogQueryParams {
    #[serde(default)]
    pub follow: bool,
}

struct JobRow {
    status: String,
    worker_ssh_ip: Option<String>,
    cached_logs: Option<String>,
}

async fn find_job(
    pool: &SqlitePool,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<JobRow>> {
    let row = sqlx::query(
        "SELECT status, worker_ssh_ip, cached_logs FROM jobs WHERE namespace = ? AND name = ?",
    )
    .bind(namespace)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| JobRow {
        status: row.get(0),
        worker_ssh_ip: row.get(1),
        cached_logs: row.get(2),
    }))
}

/// The container ID currently running on `ip`, or `None` if it can't be
/// fetched (SSH unreachable) or doesn't look like a real podman ID --
/// never interpolated into the `podman logs` command below without this
/// check passing, for the same reason `reconcile::job` validates it
/// before its own use.
async fn live_container_id(ip: &str, ssh_config: &WorkerSshConfig) -> Option<String> {
    let output = ssh::exec_once(
        ip,
        ssh_config.port,
        &ssh_config.private_key,
        "cat /tmp/container-id.txt",
    )
    .await
    .ok()?;
    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !id.is_empty() && id.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(id)
    } else {
        None
    }
}

pub async fn get_pod_log(
    State(pool): State<SqlitePool>,
    Extension(ssh_config): Extension<Arc<WorkerSshConfig>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<LogQueryParams>,
) -> Response {
    let job = match find_job(&pool, &namespace, &name).await {
        Ok(Some(job)) => job,
        Ok(None) => return k8s_status::not_found("Pod", &name),
        Err(err) => {
            tracing::error!("failed to look up pod {namespace}/{name} for logs: {err:?}");
            return k8s_status::status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "failed to look up pod",
            );
        }
    };

    let live = job.status == "ContainerRunning"
        && job.worker_ssh_ip.is_some()
        && !ssh_config.private_key.is_empty();

    if live {
        let ip = job.worker_ssh_ip.as_deref().unwrap();
        if let Some(container_id) = live_container_id(ip, &ssh_config).await {
            let command = if params.follow {
                format!("podman logs -f {container_id}")
            } else {
                format!("podman logs {container_id}")
            };

            if params.follow {
                match ssh::exec_stream(ip, ssh_config.port, &ssh_config.private_key, &command).await
                {
                    Ok(stream) => {
                        use tokio_stream::StreamExt;
                        let body_stream = stream.map(Ok::<_, std::io::Error>);
                        return Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "text/plain; charset=utf-8")
                            .body(Body::from_stream(body_stream))
                            .unwrap();
                    }
                    Err(err) => {
                        tracing::warn!(
                            "job {namespace}/{name}: live log stream from {ip} failed, falling \
                             back to cached logs: {err}"
                        );
                    }
                }
            } else {
                match ssh::exec_once(ip, ssh_config.port, &ssh_config.private_key, &command).await {
                    Ok(output) => {
                        return (
                            StatusCode::OK,
                            [("content-type", "text/plain; charset=utf-8")],
                            output.stdout,
                        )
                            .into_response();
                    }
                    Err(err) => {
                        tracing::warn!(
                            "job {namespace}/{name}: live log fetch from {ip} failed, falling \
                             back to cached logs: {err}"
                        );
                    }
                }
            }
        }
    }

    match job.cached_logs {
        Some(logs) => (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            logs,
        )
            .into_response(),
        None => k8s_status::status_error(
            StatusCode::NOT_FOUND,
            "NotFound",
            "no logs available for this pod yet",
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::ApiToken;
    use crate::ssh::WorkerSshConfig;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    // In-crate (not tests/*.rs) specifically so these can reach
    // `crate::ssh::tests::mock_ssh_server` -- a `#[cfg(test)]` item is
    // invisible to an external integration-test binary, which links
    // against this crate built *without* cfg(test).

    const TOKEN: &str = "test-token";

    async fn test_router(pool: sqlx::SqlitePool, ssh_config: WorkerSshConfig) -> axum::Router {
        crate::app::build_router(
            pool,
            Arc::new(vec![ApiToken {
                token: TOKEN.to_string(),
                expires_at: None,
            }]),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(ssh_config),
            Arc::new(crate::api::cost_report::CostReportConfig {
                resource_prefix: "kube-shim-test".to_string(),
                zone: "de-fra1".to_string(),
                main_currency: "EUR".to_string(),
                provider_name: "UpCloud".to_string(),
                invoice_issuer_name: "UpCloud Ltd".to_string(),
            }),
        )
    }

    fn log_request(name: &str, follow: bool) -> Request<Body> {
        let uri = format!("/api/v1/namespaces/default/pods/{name}/log?follow={follow}");
        Request::builder()
            .method("GET")
            .uri(uri)
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .unwrap()
    }

    fn no_ssh() -> WorkerSshConfig {
        WorkerSshConfig {
            private_key: String::new(),
            port: crate::ssh::SSH_PORT,
        }
    }

    #[tokio::test]
    async fn test_unknown_pod_is_404() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let router = test_router(pool, no_ssh()).await;

        let response = router
            .oneshot(log_request("nonexistent", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_returns_cached_logs_when_not_currently_running() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, cached_logs, created_at, updated_at, version) \
             VALUES ('j1', 'done-job', 'default', '{}', 'Archived', 'cached output\n', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let router = test_router(pool, no_ssh()).await;

        let response = router
            .oneshot(log_request("done-job", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "cached output\n");
    }

    #[tokio::test]
    async fn test_no_cache_and_not_running_is_404() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES ('j1', 'pending-job', 'default', '{}', 'VolumePending', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let router = test_router(pool, no_ssh()).await;

        let response = router
            .oneshot(log_request("pending-job", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_live_one_shot_fetch_while_container_running() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert("podman logs abc123", ("live output\n", 0));
        let (host, port) = crate::ssh::tests::mock_ssh_server(responses).await;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'running-job', 'default', '{}', 'ContainerRunning', ?, 0, 0, 1)",
        )
        .bind(&host)
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port,
        };
        let router = test_router(pool, ssh_config).await;

        let response = router
            .oneshot(log_request("running-job", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "live output\n");
    }

    #[tokio::test]
    async fn test_live_follow_streams_from_a_running_container() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        let (host, port) = crate::ssh::tests::mock_ssh_server(responses).await;

        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, created_at, updated_at, version) \
             VALUES ('j1', 'running-job', 'default', '{}', 'ContainerRunning', ?, 0, 0, 1)",
        )
        .bind(&host)
        .execute(&pool)
        .await
        .unwrap();

        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port,
        };
        let router = test_router(pool, ssh_config).await;

        let response = router
            .oneshot(log_request("running-job", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // The "exec" server used above (not the streaming one) doesn't
        // stream multiple chunks -- this only proves the follow=true path
        // is wired through to a real SSH connection and gets *a* response
        // rather than erroring; src/ssh.rs's own tests already cover the
        // actual chunk-by-chunk forwarding behavior in isolation.
        let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_ssh_ip_after_job_moved_past_container_running_uses_cache_not_live_ssh() {
        // Even though worker_ssh_ip is still set (never cleared), status
        // is no longer ContainerRunning -- must not attempt a live SSH
        // connection (see this module's own top-level docs on why a
        // stale IP is unsafe to connect to).
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, worker_ssh_ip, cached_logs, created_at, updated_at, version) \
             VALUES ('j1', 'archived-job', 'default', '{}', 'Archived', '10.0.0.99', 'the real cached output\n', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        // SSH configured with a real key, but pointed nowhere reachable --
        // if the handler tried to connect to worker_ssh_ip, this would
        // either hang or error instead of returning the cached logs.
        let ssh_config = WorkerSshConfig {
            private_key: crate::ssh::tests::throwaway_private_key_pem(),
            port: crate::ssh::SSH_PORT,
        };
        let router = test_router(pool, ssh_config).await;

        let response = router
            .oneshot(log_request("archived-job", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, "the real cached output\n");
    }
}
