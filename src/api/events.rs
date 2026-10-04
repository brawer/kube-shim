//! `kubectl describe pod`'s Events section (Phase 12). Real Kubernetes
//! events are written by many different controllers about many kinds of
//! objects; this project only ever has one kind of object worth
//! reporting on -- a job run, which is also "the pod" (see
//! `api::logs`'s own docs on that 1:1 mapping) -- so every event here has
//! exactly one possible `involvedObject.kind`: `"Pod"`.
//!
//! `events.job_id` (schema, front-loaded since Phase 1) stores the job's
//! internal UUID, not its user-facing name -- `kubectl describe pod NAME`
//! filters by `fieldSelector=involvedObject.name=NAME,involvedObject.
//! namespace=NAMESPACE`, so this module joins against `jobs` to resolve
//! that id back to a name/namespace pair, the same way `api::logs`'s
//! `find_job` already does for the log endpoint.

use super::secret::ObjectMeta;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct InvolvedObjectRef {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub namespace: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub involved_object: InvolvedObjectRef,
    pub reason: String,
    pub message: String,
    /// `serde(rename = "type")`, not the `type_field`-stays-unrenamed
    /// convention `api::secret::Secret` uses -- that field has never
    /// round-tripped through anything real, where `kubectl describe`
    /// actually reads this one to decide whether to print a line as a
    /// plain `Normal` event or call out a `Warning`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// `serde(rename)`, not snake_case like every other field here --
    /// verified hands-on that it has to be: `kubectl describe pod`'s own
    /// Events table computes its "Age" column by unmarshaling this
    /// field through client-go's typed `v1.Event` struct (real
    /// `firstTimestamp`/`lastTimestamp` JSON tags), not by passing it
    /// through opaquely the way `reason`/`message` are -- snake_case
    /// here silently produced "<unknown>" instead of an error, which is
    /// what made this easy to miss without actually running `kubectl
    /// describe` against it.
    ///
    /// Always equal to `last_timestamp`: this project records one row
    /// per occurrence rather than collapsing repeats into a `count` (see
    /// `reconcile::job::record_event`'s own docs), so there's never a
    /// first/last spread to report within a single `Event` object.
    #[serde(rename = "firstTimestamp")]
    pub first_timestamp: String,
    #[serde(rename = "lastTimestamp")]
    pub last_timestamp: String,
    pub count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Event>,
}

/// `kubectl` sends `fieldSelector=involvedObject.name=NAME,
/// involvedObject.namespace=NAMESPACE` as one comma-joined query
/// parameter (a real apiserver supports a small field-selector grammar
/// generally; this project only ever needs to recognize these two exact
/// keys, since `involvedObject.kind` is always `"Pod"` and nothing else
/// is ever selectable on an Event in a real cluster either).
fn selected_involved_object_name(field_selector: Option<&str>) -> Option<String> {
    field_selector?.split(',').find_map(|clause| {
        let (key, value) = clause.split_once('=')?;
        (key.trim() == "involvedObject.name").then(|| value.trim().to_string())
    })
}

pub async fn list_events(
    State(pool): State<SqlitePool>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<EventList>, (StatusCode, String)> {
    let involved_object_name =
        selected_involved_object_name(params.get("fieldSelector").map(String::as_str));

    let rows = sqlx::query(
        r#"
        SELECT events.id, events.reason, events.message, events.timestamp, events.type,
               jobs.name, jobs.namespace
        FROM events
        JOIN jobs ON events.job_id = jobs.id
        WHERE jobs.namespace = ? AND (? IS NULL OR jobs.name = ?)
        ORDER BY events.timestamp
        "#,
    )
    .bind(&namespace)
    .bind(involved_object_name.clone())
    .bind(involved_object_name)
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = rows
        .into_iter()
        .map(|row| {
            let id: String = row.get(0);
            let reason: String = row.get(1);
            let message: String = row.get(2);
            let timestamp: i64 = row.get(3);
            let event_type: String = row.get(4);
            let job_name: String = row.get(5);
            let job_namespace: String = row.get(6);
            // RFC 3339, not the raw-epoch-seconds-as-a-string convention
            // this project's other `creation_timestamp` fields use
            // (Secret/CronJob) -- those predate this module and nothing
            // depends on this new one matching them. `kubectl describe
            // pod`'s Events table computes its own "Age" column by
            // parsing this field as a real timestamp; verified hands-on
            // that the epoch-seconds form renders as "<unknown>" there,
            // where RFC 3339 renders correctly.
            let timestamp = chrono::DateTime::from_timestamp(timestamp, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default();

            Event {
                api_version: "v1".to_string(),
                kind: "Event".to_string(),
                metadata: ObjectMeta {
                    name: id,
                    namespace: Some(job_namespace.clone()),
                    uid: None,
                    creation_timestamp: Some(timestamp.clone()),
                },
                involved_object: InvolvedObjectRef {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    name: job_name,
                    namespace: job_namespace,
                },
                reason,
                message,
                event_type,
                first_timestamp: timestamp.clone(),
                last_timestamp: timestamp,
                count: 1,
            }
        })
        .collect();

    Ok(Json(EventList {
        api_version: "v1".to_string(),
        kind: "EventList".to_string(),
        items,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    async fn test_router(pool: SqlitePool) -> Router {
        Router::new()
            .route("/api/v1/namespaces/:namespace/events", get(list_events))
            .with_state(pool)
    }

    async fn insert_job(pool: &SqlitePool, id: &str, name: &str, namespace: &str) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version) \
             VALUES (?, ?, ?, '{}', 'Created', 0, 0, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_event(pool: &SqlitePool, job_id: &str, reason: &str, event_type: &str) {
        sqlx::query(
            "INSERT INTO events (id, job_id, reason, message, timestamp, type) \
             VALUES (?, ?, ?, 'a message', 100, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(job_id)
        .bind(reason)
        .bind(event_type)
        .execute(pool)
        .await
        .unwrap();
    }

    #[test]
    fn test_selected_involved_object_name_parses_the_real_kubectl_selector() {
        let selector = "involvedObject.name=job-one,involvedObject.namespace=default";
        assert_eq!(
            selected_involved_object_name(Some(selector)),
            Some("job-one".to_string())
        );
    }

    #[test]
    fn test_selected_involved_object_name_none_when_absent() {
        assert_eq!(selected_involved_object_name(None), None);
        assert_eq!(selected_involved_object_name(Some("foo=bar")), None);
    }

    #[tokio::test]
    async fn test_list_events_returns_events_for_the_namespace() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "job-one", "default").await;
        insert_event(&pool, "j1", "VolumeCreated", "Normal").await;
        insert_event(&pool, "j1", "Failed", "Warning").await;

        let router = test_router(pool).await;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/namespaces/default/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: EventList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].involved_object.name, "job-one");
        assert_eq!(list.items[1].event_type, "Warning");
    }

    #[tokio::test]
    async fn test_list_events_filters_by_involved_object_name() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job(&pool, "j1", "job-one", "default").await;
        insert_job(&pool, "j2", "job-two", "default").await;
        insert_event(&pool, "j1", "VolumeCreated", "Normal").await;
        insert_event(&pool, "j2", "VolumeCreated", "Normal").await;

        let router = test_router(pool).await;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/namespaces/default/events?fieldSelector=involvedObject.name%3Djob-one")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: EventList = serde_json::from_slice(&body).unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].involved_object.name, "job-one");
    }

    #[tokio::test]
    async fn test_list_events_empty_namespace_returns_empty_list() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let router = test_router(pool).await;
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/namespaces/default/events")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let list: EventList = serde_json::from_slice(&body).unwrap();
        assert!(list.items.is_empty());
    }
}
