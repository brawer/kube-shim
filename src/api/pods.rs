//! Minimal synthetic Pod GET (Phase 12) -- exists so `kubectl describe
//! pod` has a base object to fetch before it goes on to list events for
//! it (`api::events`). This project has no standalone Pod resource
//! otherwise -- `api::logs`'s own `/log` subresource is the only other
//! place "pod name" means anything (see that module's docs on the 1:1
//! job/pod mapping, which this reuses).

use super::secret::ObjectMeta;
use crate::reconcile::job::RUN_FAILED_SQL;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Serialize, Deserialize)]
pub struct PodStatus {
    pub phase: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pod {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub status: PodStatus,
}

/// Maps this project's own job state machine
/// (`reconcile::job::STATE_SEQUENCE`) onto the handful of phases a real
/// Pod reports. Everything before `ContainerRunning` is still
/// provisioning (`Pending`, same as a real pod whose containers haven't
/// started yet); the cleanup tail (`VolumeDetaching` onward) keeps
/// reporting whatever terminal phase the job already reached. `failed`
/// is `reconcile::job::RUN_FAILED_SQL` for the row, since `status`
/// itself has moved on to a cleanup state by then and no longer says
/// `Succeeded`/`Failed` directly.
///
/// `pub(crate)`: also reused by `api::job::status_for` (Phase 13) to
/// derive a standalone `Job`'s own `active`/`succeeded`/`failed`
/// status fields -- one state->outcome inference, two representations,
/// not a second copy of this same three-way mapping.
pub(crate) fn phase_for(status: &str, failed: bool) -> &'static str {
    match status {
        "ContainerRunning" => "Running",
        "Succeeded" => "Succeeded",
        "Failed" => "Failed",
        "VolumeDetaching" | "VolumeDeleted" | "VMTerminating" | "Archived" => {
            if failed {
                "Failed"
            } else {
                "Succeeded"
            }
        }
        _ => "Pending",
    }
}

pub async fn get_pod(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Pod>, (StatusCode, String)> {
    let row = sqlx::query(&format!(
        "SELECT status, {RUN_FAILED_SQL} FROM jobs WHERE namespace = ? AND name = ?"
    ))
    .bind(&namespace)
    .bind(&name)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let Some(row) = row else {
        return Err((StatusCode::NOT_FOUND, "Pod not found".to_string()));
    };
    let status: String = row.get(0);
    let failed: bool = row.get(1);

    Ok(Json(Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata: ObjectMeta {
            name,
            namespace: Some(namespace),
            uid: None,
            creation_timestamp: None,
        },
        status: PodStatus {
            phase: phase_for(&status, failed).to_string(),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn insert_job(pool: &SqlitePool, name: &str, status: &str, last_error: Option<&str>) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, last_error, created_at, updated_at, version) \
             VALUES (?, ?, 'default', '{}', ?, ?, 0, 0, 1)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(name)
        .bind(status)
        .bind(last_error)
        .execute(pool)
        .await
        .unwrap();
    }

    fn router(pool: SqlitePool) -> Router {
        Router::new()
            .route("/api/v1/namespaces/:namespace/pods/:name", get(get_pod))
            .with_state(pool)
    }

    async fn get_phase(pool: &SqlitePool, name: &str) -> String {
        let response = router(pool.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/v1/namespaces/default/pods/{name}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let pod: Pod = serde_json::from_slice(&body).unwrap();
        pod.status.phase
    }

    #[tokio::test]
    async fn test_pre_container_states_are_pending() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "VMPending", None).await;
        assert_eq!(get_phase(&pool, "p1").await, "Pending");
    }

    #[tokio::test]
    async fn test_container_running_is_running() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "ContainerRunning", None).await;
        assert_eq!(get_phase(&pool, "p1").await, "Running");
    }

    #[tokio::test]
    async fn test_cleanup_tail_after_success_stays_succeeded() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "VMTerminating", None).await;
        assert_eq!(get_phase(&pool, "p1").await, "Succeeded");
    }

    #[tokio::test]
    async fn test_cleanup_tail_after_forced_failure_stays_failed() {
        // force_failed records a Failed event; a later cleanup retry
        // overwrote last_error with something unrelated.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "VolumeDetaching", Some("delete_volume failed")).await;
        insert_failed_event(&pool, "p1").await;
        assert_eq!(get_phase(&pool, "p1").await, "Failed");
    }

    #[tokio::test]
    async fn test_archived_after_nonzero_exit_code_stays_failed() {
        // The first OSMDiffs run on the live instance (issue #120): exit
        // code 1, no last_error, then cleanup through to Archived.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "Archived", None).await;
        set_exit_code(&pool, "p1", 1).await;
        assert_eq!(get_phase(&pool, "p1").await, "Failed");
    }

    #[tokio::test]
    async fn test_archived_after_success_with_earlier_retry_stays_succeeded() {
        // A retry before completion left last_error set; the run then
        // exited 0.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "p1", "Archived", Some("create_server failed")).await;
        set_exit_code(&pool, "p1", 0).await;
        assert_eq!(get_phase(&pool, "p1").await, "Succeeded");
    }

    async fn set_exit_code(pool: &SqlitePool, name: &str, exit_code: i64) {
        sqlx::query("UPDATE jobs SET exit_code = ? WHERE name = ?")
            .bind(exit_code)
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn insert_failed_event(pool: &SqlitePool, name: &str) {
        sqlx::query(
            "INSERT INTO events (id, job_id, reason, message, timestamp, type) \
             SELECT 'e1', id, 'Failed', '', 0, 'Warning' FROM jobs WHERE name = ?",
        )
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_unknown_pod_is_404() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/namespaces/default/pods/nope")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
