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
//!
//! `timestamps`, `tailLines`, `sinceSeconds`/`sinceTime` and `limitBytes`
//! work as in Kubernetes (issue #131), on both paths: cached logs are
//! stored with podman's per-line timestamps, which are stripped unless
//! asked for. The live path always asks podman for them too, because
//! stdout and stderr arrive over SSH as separate streams and are put
//! back in time order by those timestamps (issue #139). See
//! `api::log_filter`. `previous=true` and `stream` other than `All`
//! are rejected rather than ignored.

use crate::api::log_filter::{self, LogFilter};
use crate::k8s_status;
use crate::ssh::{self, WorkerSshConfig};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;

/// The `PodLogOptions` query parameters kube-shim reads. `container` and
/// `insecureSkipTLSVerifyBackend` aren't listed: every pod has exactly
/// one container, and there is no kubelet connection to skip TLS
/// verification for.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogQueryParams {
    #[serde(default)]
    pub follow: bool,
    #[serde(default)]
    pub timestamps: bool,
    pub tail_lines: Option<i64>,
    pub since_seconds: Option<i64>,
    pub since_time: Option<String>,
    pub limit_bytes: Option<i64>,
    #[serde(default)]
    pub previous: bool,
    pub stream: Option<String>,
}

/// `LogQueryParams`, validated the way the API server validates
/// `PodLogOptions`.
struct LogOptions {
    follow: bool,
    timestamps: bool,
    tail_lines: Option<usize>,
    since: Option<DateTime<Utc>>,
    limit_bytes: Option<usize>,
}

impl LogOptions {
    /// The error is the parts of a `Status` response: code, reason and
    /// message.
    fn validate(
        params: &LogQueryParams,
        name: &str,
    ) -> Result<Self, (StatusCode, &'static str, String)> {
        let bad_request = |message: String| (StatusCode::BAD_REQUEST, "BadRequest", message);
        let unprocessable =
            |message: String| (StatusCode::UNPROCESSABLE_ENTITY, "Invalid", message);
        let invalid = |field: &str, value: &dyn std::fmt::Display, rule: &str| {
            unprocessable(format!(
                "PodLogOptions \"{name}\" is invalid: {field}: Invalid value: {value}: {rule}"
            ))
        };
        if params.previous {
            return Err(bad_request(format!(
                "previous terminated container in pod \"{name}\" not found: kube-shim never \
                 restarts a container"
            )));
        }
        if let Some(stream) = params.stream.as_deref() {
            if stream != "All" {
                return Err(bad_request(format!(
                    "stream \"{stream}\" is not supported: kube-shim returns stdout and \
                     stderr merged (stream=All)"
                )));
            }
        }
        if params.since_seconds.is_some() && params.since_time.is_some() {
            return Err(unprocessable(format!(
                "PodLogOptions \"{name}\" is invalid: : Forbidden: at most one of `sinceTime` \
                 or `sinceSeconds` may be specified"
            )));
        }
        let tail_lines = match params.tail_lines {
            Some(n) if n < 0 => {
                return Err(invalid(
                    "tailLines",
                    &n,
                    "must be greater than or equal to 0",
                ))
            }
            n => n.map(|n| n as usize),
        };
        let limit_bytes = match params.limit_bytes {
            Some(n) if n < 1 => return Err(invalid("limitBytes", &n, "must be greater than 0")),
            n => n.map(|n| n as usize),
        };
        let since = match (params.since_seconds, params.since_time.as_deref()) {
            (Some(n), _) if n < 1 => {
                return Err(invalid("sinceSeconds", &n, "must be greater than 0"))
            }
            (Some(n), _) => Some(Utc::now() - chrono::Duration::seconds(n)),
            (None, Some(time)) => match DateTime::parse_from_rfc3339(time) {
                Ok(time) => Some(time.with_timezone(&Utc)),
                Err(_) => {
                    return Err(bad_request(format!(
                        "sinceTime \"{time}\" is not an RFC3339 time"
                    )))
                }
            },
            (None, None) => None,
        };
        Ok(LogOptions {
            follow: params.follow,
            timestamps: params.timestamps,
            tail_lines,
            since,
            limit_bytes,
        })
    }

    /// The filter for logs that do (or don't) carry podman timestamps.
    fn filter(&self, input_has_timestamps: bool) -> LogFilter {
        LogFilter {
            input_has_timestamps,
            timestamps: self.timestamps,
            since: self.since,
            limit_bytes: self.limit_bytes,
        }
    }

    /// The live `podman logs` command for `container_id`. Always with
    /// `--timestamps`, to merge stdout and stderr in time order.
    fn podman_command(&self, container_id: &str) -> String {
        let mut command = "podman logs".to_string();
        if self.follow {
            command.push_str(" -f");
        }
        command.push_str(" --timestamps");
        if let Some(n) = self.tail_lines {
            command.push_str(&format!(" --tail {n}"));
        }
        command.push(' ');
        command.push_str(container_id);
        command
    }
}

struct JobRow {
    status: String,
    worker_ssh_ip: Option<String>,
    cached_logs: Option<String>,
    cached_logs_timestamped: bool,
}

async fn find_job(
    pool: &SqlitePool,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<JobRow>> {
    let row = sqlx::query(
        "SELECT status, worker_ssh_ip, cached_logs, cached_logs_timestamped FROM jobs \
         WHERE namespace = ? AND name = ?",
    )
    .bind(namespace)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| JobRow {
        status: row.get(0),
        worker_ssh_ip: row.get(1),
        cached_logs: row.get(2),
        cached_logs_timestamped: row.get::<i64, _>(3) != 0,
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
    let options = match LogOptions::validate(&params, &name) {
        Ok(options) => options,
        Err((status, reason, message)) => return k8s_status::status_error(status, reason, message),
    };
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
            let command = options.podman_command(&container_id);
            let filter = options.filter(true);

            if options.follow {
                match ssh::exec_stream(ip, ssh_config.port, &ssh_config.private_key, &command).await
                {
                    Ok(stream) => {
                        use tokio_stream::StreamExt;
                        let body_stream = filter.apply_stream(stream).map(Ok::<_, std::io::Error>);
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
                            // podman already applied --tail.
                            filter.apply(
                                &log_filter::merge_by_timestamp(&[&output.stdout, &output.stderr]),
                                None,
                            ),
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
        // Logs cached before issue #131 have no timestamps: returned
        // without them even for timestamps=true, and not filtered by
        // since*.
        Some(logs) => (
            StatusCode::OK,
            [("content-type", "text/plain; charset=utf-8")],
            options
                .filter(job.cached_logs_timestamped)
                .apply(logs.as_bytes(), options.tail_lines),
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
            Arc::new(crate::reconcile::TickHealth::new()),
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

    fn log_request_query(name: &str, query: &str) -> Request<Body> {
        let uri = format!("/api/v1/namespaces/default/pods/{name}/log?{query}");
        Request::builder()
            .method("GET")
            .uri(uri)
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .unwrap()
    }

    async fn body_of(router: axum::Router, request: Request<Body>) -> (StatusCode, String) {
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    const TIMESTAMPED: &str = "2026-11-07T10:26:27.501234567Z first\n\
                               2026-11-07T10:26:28.501234567Z second\n\
                               2026-11-07T10:26:29.501234567Z third\n";

    /// An archived job whose cached logs are `logs`, with
    /// `cached_logs_timestamped = timestamped`.
    async fn archived_job_router(logs: &str, timestamped: bool) -> axum::Router {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, cached_logs, \
             cached_logs_timestamped, created_at, updated_at, version) \
             VALUES ('j1', 'done-job', 'default', '{}', 'Archived', ?, ?, 0, 0, 1)",
        )
        .bind(logs)
        .bind(timestamped)
        .execute(&pool)
        .await
        .unwrap();
        test_router(pool, no_ssh()).await
    }

    #[tokio::test]
    async fn test_cached_logs_with_timestamps() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (status, body) =
            body_of(router, log_request_query("done-job", "timestamps=true")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, TIMESTAMPED);
    }

    #[tokio::test]
    async fn test_cached_logs_without_timestamps_param_are_stripped() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (status, body) = body_of(router, log_request("done-job", false)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "first\nsecond\nthird\n");
    }

    #[tokio::test]
    async fn test_cached_logs_from_v0_4_2_are_sorted_by_timestamp() {
        // Cached by v0.4.2, before issue #139: all of stdout, then stderr.
        let logs = "2026-11-07T12:30:32.473409000Z first line\n\
                    2026-11-07T12:30:33.473398000Z second line\n\
                    2026-11-07T12:30:43.489121000Z timed out\n\
                    2026-11-07T12:30:33.473721000Z to stderr\n";
        let router = archived_job_router(logs, true).await;
        let (status, body) = body_of(router, log_request("done-job", false)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "first line\nsecond line\nto stderr\ntimed out\n");

        let router = archived_job_router(logs, true).await;
        let (_, body) = body_of(router, log_request_query("done-job", "tailLines=1")).await;
        assert_eq!(body, "timed out\n");
    }

    #[tokio::test]
    async fn test_old_cached_logs_are_returned_as_is_for_timestamps_true() {
        // Cached before issue #131: no timestamps, even when a line looks
        // like it starts with one.
        let logs = "2026-01-01T00:00:00Z printed by the job itself\n";
        let router = archived_job_router(logs, false).await;
        let (_, body) = body_of(router, log_request_query("done-job", "timestamps=true")).await;
        assert_eq!(body, logs);
        let router = archived_job_router(logs, false).await;
        let (_, body) = body_of(router, log_request("done-job", false)).await;
        assert_eq!(body, logs);
    }

    #[tokio::test]
    async fn test_cached_logs_tail_lines() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (_, body) = body_of(router, log_request_query("done-job", "tailLines=2")).await;
        assert_eq!(body, "second\nthird\n");
    }

    #[tokio::test]
    async fn test_cached_logs_since_time() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (_, body) = body_of(
            router,
            log_request_query("done-job", "sinceTime=2026-11-07T10:26:28Z&timestamps=true"),
        )
        .await;
        assert_eq!(
            body,
            "2026-11-07T10:26:28.501234567Z second\n2026-11-07T10:26:29.501234567Z third\n"
        );
    }

    #[tokio::test]
    async fn test_cached_logs_since_seconds_drops_old_lines() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (_, body) = body_of(router, log_request_query("done-job", "sinceSeconds=60")).await;
        assert_eq!(body, "");
    }

    #[tokio::test]
    async fn test_cached_logs_limit_bytes() {
        let router = archived_job_router(TIMESTAMPED, true).await;
        let (_, body) = body_of(router, log_request_query("done-job", "limitBytes=8")).await;
        assert_eq!(body, "first\nse");
    }

    #[tokio::test]
    async fn test_invalid_and_unsupported_options_are_rejected() {
        for (query, expected) in [
            ("tailLines=-1", StatusCode::UNPROCESSABLE_ENTITY),
            ("limitBytes=0", StatusCode::UNPROCESSABLE_ENTITY),
            ("sinceSeconds=0", StatusCode::UNPROCESSABLE_ENTITY),
            (
                "sinceSeconds=5&sinceTime=2026-11-07T10:26:28Z",
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            ("sinceTime=yesterday", StatusCode::BAD_REQUEST),
            ("previous=true", StatusCode::BAD_REQUEST),
            ("stream=Stdout", StatusCode::BAD_REQUEST),
        ] {
            let router = archived_job_router(TIMESTAMPED, true).await;
            let (status, body) = body_of(router, log_request_query("done-job", query)).await;
            assert_eq!(status, expected, "{query}");
            let json: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(json["kind"], "Status", "{query}");
        }
        for query in ["previous=false", "stream=All"] {
            let router = archived_job_router(TIMESTAMPED, true).await;
            let (status, _) = body_of(router, log_request_query("done-job", query)).await;
            assert_eq!(status, StatusCode::OK, "{query}");
        }
    }

    async fn running_job_router(
        responses: HashMap<&'static str, (&'static str, u32)>,
    ) -> axum::Router {
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
        test_router(pool, ssh_config).await
    }

    #[tokio::test]
    async fn test_live_one_shot_with_timestamps_normalizes_them() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert(
            "podman logs --timestamps abc123",
            (
                "2026-11-07T10:26:27.5+00:00 a\n2026-11-07T10:26:28.5+00:00 b\n",
                0,
            ),
        );
        let router = running_job_router(responses).await;
        let (status, body) =
            body_of(router, log_request_query("running-job", "timestamps=true")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "2026-11-07T10:26:27.500000000Z a\n2026-11-07T10:26:28.500000000Z b\n"
        );
    }

    #[tokio::test]
    async fn test_live_follow_with_timestamps_and_tail() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert(
            "podman logs -f --timestamps --tail 1 abc123",
            ("2026-11-07T10:26:28.5+00:00 b\n", 0),
        );
        let router = running_job_router(responses).await;
        let (status, body) = body_of(
            router,
            log_request_query("running-job", "follow=true&timestamps=true&tailLines=1"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "2026-11-07T10:26:28.500000000Z b\n");
    }

    #[tokio::test]
    async fn test_live_since_fetches_timestamps_to_filter_then_strips_them() {
        let mut responses = HashMap::new();
        responses.insert("cat /tmp/container-id.txt", ("abc123", 0));
        responses.insert(
            "podman logs --timestamps abc123",
            (
                "2026-11-07T10:26:27+00:00 old\n2026-11-07T10:26:29+00:00 new\n",
                0,
            ),
        );
        let router = running_job_router(responses).await;
        let (_, body) = body_of(
            router,
            log_request_query("running-job", "sinceTime=2026-11-07T10:26:28Z"),
        )
        .await;
        assert_eq!(body, "new\n");
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
        responses.insert(
            "podman logs --timestamps abc123",
            ("2026-11-07T10:26:27+00:00 live output\n", 0),
        );
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

    /// `running_job_router_with_logs`' output, merged in time order.
    const IN_TIME_ORDER: &str = "first line\nsecond line\nto stderr\nnameserver 94.237.127.9\n";

    async fn running_job_router_with_logs(follow_command: &'static str) -> axum::Router {
        use crate::ssh::tests::MockOut;
        let mut responses = HashMap::new();
        responses.insert(
            "cat /tmp/container-id.txt",
            (vec![MockOut::Stdout("abc123")], 0),
        );
        responses.insert(
            follow_command,
            (
                vec![
                    // Issue #139: SSH delivers all of stdout before
                    // stderr's line that was written in between.
                    MockOut::Stdout(
                        "2026-11-07T12:30:32.473409+00:00 first line\n\
                         2026-11-07T12:30:33.473398+00:00 second line\n\
                         2026-11-07T12:30:33.475578+00:00 nameserver 94.237.127.9\n",
                    ),
                    MockOut::Stderr("2026-11-07T12:30:33.473721+00:00 to stderr\n"),
                ],
                0,
            ),
        );
        let (host, port) = crate::ssh::tests::mock_ssh_server_with_output(responses).await;

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
        test_router(pool, ssh_config).await
    }

    #[tokio::test]
    async fn test_live_one_shot_fetch_includes_stderr_in_time_order() {
        let router = running_job_router_with_logs("podman logs --timestamps abc123").await;

        let response = router
            .oneshot(log_request("running-job", false))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, IN_TIME_ORDER);
    }

    #[tokio::test]
    async fn test_live_follow_includes_stderr_in_time_order() {
        let router = running_job_router_with_logs("podman logs -f --timestamps abc123").await;

        let response = router
            .oneshot(log_request("running-job", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, IN_TIME_ORDER);
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
