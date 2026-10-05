//! Minimal synthetic Pod GET (Phase 12) -- exists so `kubectl describe
//! pod` has a base object to fetch before it goes on to list events for
//! it (`api::events`). This project has no standalone Pod resource
//! otherwise -- `api::logs`'s own `/log` subresource is the only other
//! place "pod name" means anything (see that module's docs on the 1:1
//! job/pod mapping, which this reuses).

use super::secret::ObjectMeta;
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
/// reporting whatever terminal phase the job already reached, inferred
/// from `last_error` since `status` itself has moved on to a cleanup
/// state by then and no longer says `Succeeded`/`Failed` directly.
///
/// `pub(crate)`: also reused by `api::job::status_for` (Phase 13) to
/// derive a standalone `Job`'s own `active`/`succeeded`/`failed`
/// status fields -- one state->outcome inference, two representations,
/// not a second copy of this same three-way mapping.
pub(crate) fn phase_for(status: &str, last_error: &Option<String>) -> &'static str {
    match status {
        "ContainerRunning" => "Running",
        "Succeeded" => "Succeeded",
        "Failed" => "Failed",
        "VolumeDetaching" | "VolumeDeleted" | "VMTerminating" | "Archived" => {
            if last_error.is_some() {
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
    let row = sqlx::query("SELECT status, last_error FROM jobs WHERE namespace = ? AND name = ?")
        .bind(&namespace)
        .bind(&name)
        .fetch_optional(&pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let Some(row) = row else {
        return Err((StatusCode::NOT_FOUND, "Pod not found".to_string()));
    };
    let status: String = row.get(0);
    let last_error: Option<String> = row.get(1);

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
            phase: phase_for(&status, &last_error).to_string(),
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
    async fn test_cleanup_tail_after_failure_stays_failed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(
            &pool,
            "p1",
            "VolumeDetaching",
            Some("DeadlineExceeded: ..."),
        )
        .await;
        assert_eq!(get_phase(&pool, "p1").await, "Failed");
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
