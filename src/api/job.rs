//! Standalone `batch/v1` `Job` (Phase 13) -- a job run that exists
//! directly, not only ever spawned indirectly by a `CronJob`'s own
//! schedule (`reconcile::schedule`). A real, user-surfaced need: testing
//! a new workload version/release by hand before trusting it to run
//! unattended as a recurring `CronJob`.
//!
//! Structurally this is nothing more than another `jobs` row with
//! `cronjob_name = NULL`: every real mechanism downstream --
//! `reconcile::job::advance_all`'s state machine, VM provisioning,
//! volumes, SSH/logs, events, metrics collection -- already operates
//! purely on a row's own `id`/`status`/`namespace`/`name`, with no
//! branch anywhere on `cronjob_name`. `jobs.spec` already stores the
//! unwrapped pod-template-level spec for a CronJob-spawned run
//! (`reconcile::schedule::create_job_run`) -- exactly the shape a
//! standalone Job's own top-level `.spec` already is, so no translation
//! is needed here either. See docs/IMPLEMENTATION_PLAN.md Phase 13.

use super::pods::phase_for;
use super::secret::ObjectMeta;
use crate::reconcile::job as reconcile_job;
use crate::{admission, k8s_status};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{sqlite::SqliteRow, Row, SqlitePool};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

/// A standalone Job's own `.spec` is already the pod-template-level
/// spec `admission`'s checks expect -- see this module's own top-level
/// docs -- so its field-path prefix in their rejection messages is just
/// `"spec"`, not `"spec.jobTemplate.spec"` the way `api::cronjob`'s
/// call site needs.
const FIELD_PREFIX: &str = "spec";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
    pub status: JobStatus,
}

/// Real `batch/v1` `JobStatus`'s own `active`/`succeeded`/`failed`
/// counts (always 0 or 1 here -- this project never runs more than one
/// pod per job) plus `conditions`, not a `Pod`-style single `phase`
/// string -- `kubectl get jobs`/`kubectl describe job` read this shape
/// specifically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatus {
    pub active: i32,
    pub succeeded: i32,
    pub failed: i32,
    pub conditions: Vec<JobCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateJobRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
}

#[derive(Debug, Serialize)]
pub struct JobList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Job>,
}

/// Mirrors `api::pods::phase_for`'s own state->outcome inference, in
/// `JobStatus`'s own `active`/`succeeded`/`failed`/`conditions` shape
/// instead of `Pod`'s single `phase` string -- one inference, two
/// representations, not a second copy of this same three-way mapping.
fn status_for(status: &str, last_error: &Option<String>) -> JobStatus {
    match phase_for(status, last_error) {
        "Succeeded" => JobStatus {
            active: 0,
            succeeded: 1,
            failed: 0,
            conditions: vec![JobCondition {
                condition_type: "Complete".to_string(),
                status: "True".to_string(),
            }],
        },
        "Failed" => JobStatus {
            active: 0,
            succeeded: 0,
            failed: 1,
            conditions: vec![JobCondition {
                condition_type: "Failed".to_string(),
                status: "True".to_string(),
            }],
        },
        // "Running" and "Pending" (still provisioning) -- real
        // Kubernetes' own Job.status.active already collapses "pod
        // scheduled but not yet running" and "pod running" into the
        // same count, so this isn't a simplification specific to this
        // project.
        _ => JobStatus {
            active: 1,
            succeeded: 0,
            failed: 0,
            conditions: vec![],
        },
    }
}

fn row_to_job(row: SqliteRow) -> Job {
    let id: String = row.get(0);
    let name: String = row.get(1);
    let namespace: String = row.get(2);
    let spec_str: String = row.get(3);
    let status: String = row.get(4);
    let last_error: Option<String> = row.get(5);

    let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);

    Job {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        metadata: ObjectMeta {
            name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: None,
        },
        spec,
        status: status_for(&status, &last_error),
    }
}

pub async fn create_job(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Json(req): Json<CreateJobRequest>,
) -> Response {
    // Admission checks first, before anything is written -- same
    // ordering api::cronjob::create_cronjob uses (validate, then
    // persist). req.spec is already the pod-template-level spec these
    // checks expect -- see this module's own top-level docs.
    let object_description = format!("Job \"{}\"", req.metadata.name);
    if let Some(response) = admission::require_active_deadline_seconds(&req.spec, FIELD_PREFIX) {
        return response;
    }
    if let Some(response) = admission::validate_ephemeral_volume_storage_classes(
        &object_description,
        &req.spec,
        FIELD_PREFIX,
    ) {
        return response;
    }
    if let Some(response) =
        admission::validate_resource_limits(&object_description, &req.spec, FIELD_PREFIX)
    {
        return response;
    }

    let namespace = req
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());

    // A CronJob-spawned run's name is always timestamp-suffixed
    // (reconcile::schedule::create_job_run), so a collision can't
    // happen there -- a standalone Job's name is whatever the caller
    // gave it, so this needs an explicit check, matching real
    // Kubernetes' own per-namespace Job-name-uniqueness.
    let existing = sqlx::query("SELECT 1 FROM jobs WHERE name = ? AND namespace = ?")
        .bind(&req.metadata.name)
        .bind(&namespace)
        .fetch_optional(&pool)
        .await;
    match existing {
        Ok(Some(_)) => return k8s_status::already_exists("Job", &req.metadata.name),
        Ok(None) => {}
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }

    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let spec_json = match serde_json::to_string(&req.spec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // cronjob_name is left unbound -- NULL, same as any column omitted
    // from an INSERT -- which is exactly what distinguishes this row
    // from a CronJob-spawned run everywhere else that matters
    // (reconcile::metrics_collector/api::metrics's own is_pre_completion_state
    // reuse neither know nor care, by design).
    let insert_result = sqlx::query(
        "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
         VALUES (?, ?, ?, ?, 'Created', ?, ?, 1)",
    )
    .bind(&id)
    .bind(&req.metadata.name)
    .bind(&namespace)
    .bind(&spec_json)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await;

    if let Err(e) = insert_result {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Wake the reconciliation loop immediately rather than waiting up to
    // reconcile::FALLBACK_INTERVAL for it to notice this Job on its next
    // poll -- same reasoning create_cronjob's own notify_one() call has.
    notify.notify_one();

    let job = Job {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        metadata: ObjectMeta {
            name: req.metadata.name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: Some(now.to_string()),
        },
        spec: req.spec,
        status: status_for("Created", &None),
    };

    (StatusCode::CREATED, Json(job)).into_response()
}

/// All `jobs` rows in the namespace, regardless of `cronjob_name` --
/// real `kubectl get jobs` lists every Job, including one owned by a
/// CronJob (it's still its own independently-listed object there, not
/// hidden from this list just because something else spawned it).
pub async fn list_jobs(
    State(pool): State<SqlitePool>,
    Path(namespace): Path<String>,
) -> Result<Json<JobList>, (StatusCode, String)> {
    let rows = sqlx::query(
        "SELECT id, name, namespace, spec, status, last_error FROM jobs WHERE namespace = ?",
    )
    .bind(&namespace)
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = rows.into_iter().map(row_to_job).collect();

    Ok(Json(JobList {
        api_version: "batch/v1".to_string(),
        kind: "JobList".to_string(),
        items,
    }))
}

pub async fn get_job(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Job>, (StatusCode, String)> {
    let row = sqlx::query(
        "SELECT id, name, namespace, spec, status, last_error FROM jobs \
         WHERE namespace = ? AND name = ?",
    )
    .bind(&namespace)
    .bind(&name)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .ok_or_else(|| (StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    Ok(Json(row_to_job(row)))
}

/// A still-running Job (anything `reconcile::job::is_pre_completion_state`)
/// is routed through the exact same `force_failed` cleanup path
/// deadline/stuck-state enforcement (Phase 11) already uses -- one real
/// teardown mechanism, not a second one built just for user-initiated
/// delete, so the worker VM/volume actually get torn down rather than
/// abandoned. An already-finished Job (Succeeded/Failed/cleanup
/// tail/Archived) has nothing real left to protect by keeping the row
/// around, so it's deleted outright immediately, same as
/// `api::cronjob::delete_cronjob`'s own immediate-removal behavior.
pub async fn delete_job(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let row = sqlx::query("SELECT id, status FROM jobs WHERE namespace = ? AND name = ?")
        .bind(&namespace)
        .bind(&name)
        .fetch_optional(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let Some(row) = row else {
        return Err((StatusCode::NOT_FOUND, "Job not found".to_string()));
    };
    let id: String = row.get(0);
    let status: String = row.get(1);

    if reconcile_job::is_pre_completion_state(&status) {
        reconcile_job::force_failed(
            &pool,
            &id,
            &namespace,
            &name,
            "Deleted: job was explicitly deleted while still running",
            Utc::now().timestamp(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    } else {
        sqlx::query("DELETE FROM jobs WHERE id = ?")
            .bind(&id)
            .execute(&pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        routing::{get, post},
        Router,
    };
    use serde_json::json;
    use tower::ServiceExt;

    fn router(pool: SqlitePool) -> Router {
        Router::new()
            .route(
                "/apis/batch/v1/namespaces/:namespace/jobs",
                post(create_job).get(list_jobs),
            )
            .route(
                "/apis/batch/v1/namespaces/:namespace/jobs/:name",
                get(get_job).delete(delete_job),
            )
            .layer(Extension(Arc::new(Notify::new())))
            .with_state(pool)
    }

    fn create_request(name: &str, spec: JsonValue) -> axum::http::Request<axum::body::Body> {
        let body = json!({
            "api_version": "batch/v1",
            "kind": "Job",
            "metadata": {"name": name, "namespace": "default"},
            "spec": spec,
        });
        axum::http::Request::builder()
            .method("POST")
            .uri("/apis/batch/v1/namespaces/default/jobs")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    }

    fn valid_spec() -> JsonValue {
        json!({
            "activeDeadlineSeconds": 300,
            "template": {"spec": {
                "containers": [{"name": "main", "image": "busybox:latest"}],
                "restartPolicy": "Never"
            }}
        })
    }

    async fn body_json(response: Response) -> JsonValue {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn test_create_job_succeeds_and_inserts_with_no_cronjob_name() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool.clone())
            .oneshot(create_request("hello-world-test", valid_spec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_json(response).await;
        assert_eq!(body["metadata"]["name"], "hello-world-test");
        assert_eq!(body["status"]["active"], 1);

        let row = sqlx::query("SELECT status, cronjob_name FROM jobs WHERE name = ?")
            .bind("hello-world-test")
            .fetch_one(&pool)
            .await
            .unwrap();
        let status: String = row.get(0);
        let cronjob_name: Option<String> = row.get(1);
        assert_eq!(status, "Created");
        assert_eq!(cronjob_name, None);
    }

    #[tokio::test]
    async fn test_create_job_without_active_deadline_seconds_rejected() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let spec = json!({"template": {"spec": {
            "containers": [{"name": "main", "image": "busybox:latest"}]
        }}});
        let response = router(pool)
            .oneshot(create_request("missing-deadline", spec))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = body_json(response).await;
        // Field path must be "spec.activeDeadlineSeconds", not the
        // CronJob caller's "spec.jobTemplate.spec.activeDeadlineSeconds".
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("spec.activeDeadlineSeconds"));
        assert!(!message.contains("jobTemplate"));
    }

    #[tokio::test]
    async fn test_create_job_duplicate_name_in_same_namespace_rejected() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let first = router(pool.clone())
            .oneshot(create_request("dup", valid_spec()))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);

        let second = router(pool)
            .oneshot(create_request("dup", valid_spec()))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::CONFLICT);
        let body = body_json(second).await;
        assert_eq!(body["reason"], "AlreadyExists");
    }

    async fn insert_job_with_status(pool: &SqlitePool, name: &str, status: &str) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES (?, ?, 'default', '{}', ?, 0, 0, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(name)
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_list_jobs_includes_cronjob_spawned_runs_too() {
        // A CronJob-spawned run is still its own real, independently
        // listed Job -- not filtered out just because something else
        // created it.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(&pool, "standalone-one", "ContainerRunning").await;
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, cronjob_name, created_at, updated_at, version) \
             VALUES (?, 'owned-run', 'default', '{}', 'Succeeded', 'my-cronjob', 0, 0, 1)",
        )
        .bind(Uuid::new_v4().to_string())
        .execute(&pool)
        .await
        .unwrap();

        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/apis/batch/v1/namespaces/default/jobs")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        let names: Vec<&str> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["metadata"]["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"standalone-one"));
        assert!(names.contains(&"owned-run"));
    }

    #[tokio::test]
    async fn test_get_job_status_mapping() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(&pool, "succeeded-one", "Succeeded").await;

        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/apis/batch/v1/namespaces/default/jobs/succeeded-one")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_json(response).await;
        assert_eq!(body["status"]["succeeded"], 1);
        assert_eq!(body["status"]["active"], 0);
        assert_eq!(body["status"]["conditions"][0]["type"], "Complete");
    }

    #[tokio::test]
    async fn test_get_unknown_job_is_404() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/apis/batch/v1/namespaces/default/jobs/nope")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_still_running_job_force_fails_instead_of_removing_the_row() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(&pool, "running-one", "ContainerRunning").await;

        let response = router(pool.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("DELETE")
                    .uri("/apis/batch/v1/namespaces/default/jobs/running-one")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Routed through force_failed, not deleted outright: the row
        // still exists, now Failed, ready to rejoin the real cleanup
        // path (VolumeDetaching -> ... -> Archived) on the next tick.
        let row = sqlx::query("SELECT status, last_error FROM jobs WHERE name = 'running-one'")
            .fetch_one(&pool)
            .await
            .unwrap();
        let status: String = row.get(0);
        let last_error: Option<String> = row.get(1);
        assert_eq!(status, "Failed");
        assert!(last_error.unwrap().contains("Deleted"));
    }

    #[tokio::test]
    async fn test_delete_already_finished_job_removes_the_row() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(&pool, "done-one", "Archived").await;

        let response = router(pool.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("DELETE")
                    .uri("/apis/batch/v1/namespaces/default/jobs/done-one")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE name = 'done-one'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_delete_unknown_job_is_404() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .method("DELETE")
                    .uri("/apis/batch/v1/namespaces/default/jobs/nope")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
