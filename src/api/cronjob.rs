use super::cost_report::CostReportConfig;
use super::secret::ObjectMeta;
use crate::admission;
use crate::k8s_status;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronJob {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateCronJobRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
}

pub async fn create_cronjob(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Extension(cost_report_config): Extension<Arc<CostReportConfig>>,
    Json(req): Json<CreateCronJobRequest>,
) -> Response {
    // Admission checks first, before anything is written -- same ordering
    // a real cluster's admission chain uses (validate, then persist), and
    // it means neither check needs to worry about cleaning up a
    // half-created row on rejection.
    if let Some(response) = admit_cronjob(
        &pool,
        &cost_report_config.zone,
        &req.metadata.name,
        &req.spec,
    )
    .await
    {
        return response;
    }

    let namespace = req
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    // Kubernetes' per-namespace name uniqueness, as `api::job::create_job`
    // already does for Jobs. Without it, a second POST would add a second
    // row that the scheduler runs alongside the first (issue #105), and
    // an update by name couldn't say which one it meant.
    match cronjob_exists(&pool, &namespace, &req.metadata.name).await {
        Ok(true) => return k8s_status::already_exists("CronJob", &req.metadata.name),
        Ok(false) => {}
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }

    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let spec_json = match serde_json::to_string(&req.spec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    // Phase 6's reconciliation loop (src/reconcile/schedule.rs) needs the
    // real schedule to check due-ness against -- this used to be hardcoded
    // to "" ("will be extracted... in Phase 2", a stale comment from an
    // earlier phase numbering that never actually did it).
    let schedule = schedule_of(&req.spec);

    let insert_result = sqlx::query(
        r#"
        INSERT INTO cronjobs (id, name, namespace, spec, created_at, updated_at, schedule, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(&req.metadata.name)
    .bind(&namespace)
    .bind(&spec_json)
    .bind(now)
    .bind(now)
    .bind(&schedule)
    .bind(1)
    .execute(&pool)
    .await;

    if let Err(e) = insert_result {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Wake the reconciliation loop immediately rather than waiting up to
    // reconcile::FALLBACK_INTERVAL for it to notice this CronJob's
    // schedule on its next poll.
    notify.notify_one();

    let cronjob = CronJob {
        api_version: "batch/v1".to_string(),
        kind: "CronJob".to_string(),
        metadata: ObjectMeta {
            name: req.metadata.name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: Some(super::secret::rfc3339(now)),
        },
        spec: req.spec,
        status: None,
    };

    (StatusCode::CREATED, Json(cronjob)).into_response()
}

pub async fn get_cronjob(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<CronJob>, (StatusCode, String)> {
    load_cronjob(&pool, &namespace, &name)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "CronJob not found".to_string()))
}

/// `PUT .../cronjobs/{name}` (issue #105): replaces the CronJob's whole
/// `spec`, which is how the `hashicorp/kubernetes` provider applies any
/// in-place change (e.g. flipping `suspend`). Runs the same admission
/// checks as create. Only future runs see the new spec: each job run
/// keeps its own copy of `jobTemplate.spec` from when it was created,
/// the same as in Kubernetes. `metadata.resourceVersion` isn't checked
/// (kube-shim doesn't hand one out), so every update is unconditional,
/// which CronJobs allow in Kubernetes too.
pub async fn replace_cronjob(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Extension(cost_report_config): Extension<Arc<CostReportConfig>>,
    Path((namespace, name)): Path<(String, String)>,
    Json(req): Json<CreateCronJobRequest>,
) -> Response {
    if req.metadata.name != name {
        return k8s_status::bad_request(format!(
            "the name of the object ({}) does not match the name on the URL ({name})",
            req.metadata.name
        ));
    }
    if req
        .metadata
        .namespace
        .as_deref()
        .is_some_and(|ns| ns != namespace)
    {
        return k8s_status::bad_request(
            "the namespace of the provided object does not match the namespace sent on the request",
        );
    }
    update_cronjob(
        &pool,
        &notify,
        &cost_report_config.zone,
        &namespace,
        &name,
        req.spec,
    )
    .await
}

/// `PATCH .../cronjobs/{name}` (issue #105), JSON merge patch only
/// (RFC 7386, `application/merge-patch+json`), e.g.
/// `kubectl patch cronjob x --type merge -p '{"spec":{"suspend":false}}'`.
/// Plain `kubectl patch` sends a strategic merge patch, which merges
/// lists by key (containers by name, and so on); treating it as a merge
/// patch would silently replace whole lists instead, so it's refused
/// with a hint. The patched object then goes through the same path as
/// a `PUT`.
pub async fn patch_cronjob(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Extension(cost_report_config): Extension<Arc<CostReportConfig>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::trim)
        .unwrap_or("");
    if content_type != "application/merge-patch+json" {
        return k8s_status::unsupported_media_type(format!(
            "the body of the request was in an unknown format - accepted media types \
             include: application/merge-patch+json (got {content_type:?}; with kubectl, \
             use `kubectl patch --type merge`)"
        ));
    }
    let patch: JsonValue = match serde_json::from_slice(&body) {
        Ok(patch) => patch,
        Err(e) => return k8s_status::bad_request(format!("invalid merge patch: {e}")),
    };

    let current = match load_cronjob(&pool, &namespace, &name).await {
        Ok(Some(current)) => current,
        Ok(None) => return k8s_status::not_found("CronJob", &name),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let mut object = json!({
        "apiVersion": current.api_version,
        "kind": current.kind,
        "metadata": {"name": name, "namespace": namespace},
        "spec": current.spec,
    });
    merge_patch(&mut object, &patch);

    if object.pointer("/metadata/name").and_then(JsonValue::as_str) != Some(name.as_str())
        || object
            .pointer("/metadata/namespace")
            .and_then(JsonValue::as_str)
            != Some(namespace.as_str())
    {
        return k8s_status::bad_request("a patch can't change a CronJob's name or namespace");
    }
    let spec = object.get("spec").cloned().unwrap_or(JsonValue::Null);
    update_cronjob(
        &pool,
        &notify,
        &cost_report_config.zone,
        &namespace,
        &name,
        spec,
    )
    .await
}

/// The shared tail of `replace_cronjob` and `patch_cronjob`: admission,
/// then store the new spec and schedule, keeping the row's id and
/// `created_at` (the scheduler's fallback for "last run" on a CronJob
/// that has never run). Returns 200 with the updated object, as
/// Kubernetes does.
async fn update_cronjob(
    pool: &SqlitePool,
    notify: &Notify,
    zone: &str,
    namespace: &str,
    name: &str,
    spec: JsonValue,
) -> Response {
    if let Some(response) = admit_cronjob(pool, zone, name, &spec).await {
        return response;
    }
    let spec_json = match serde_json::to_string(&spec) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let result = sqlx::query(
        r#"
        UPDATE cronjobs
        SET spec = ?, schedule = ?, updated_at = ?, version = version + 1
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(&spec_json)
    .bind(schedule_of(&spec))
    .bind(Utc::now().timestamp())
    .bind(name)
    .bind(namespace)
    .execute(pool)
    .await;
    match result {
        Ok(done) if done.rows_affected() == 0 => return k8s_status::not_found("CronJob", name),
        Ok(_) => {}
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }

    // Unsuspending (or a schedule change) can make a run due right away.
    notify.notify_one();

    match load_cronjob(pool, namespace, name).await {
        Ok(Some(cronjob)) => (StatusCode::OK, Json(cronjob)).into_response(),
        Ok(None) => k8s_status::not_found("CronJob", name),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Every admission check a CronJob's spec must pass, on create and on
/// update alike.
///
/// The pod-template-level checks take `jobTemplate.spec` directly plus
/// this field-path prefix for their own rejection messages --
/// api::job::create_job is the other caller, passing its own Job's
/// `.spec` (no jobTemplate wrapper) and "spec" instead. Missing/malformed
/// jobTemplate becomes JsonValue::Null, which every check already treats
/// as "nothing set" rather than panicking.
async fn admit_cronjob(
    pool: &SqlitePool,
    zone: &str,
    name: &str,
    spec: &JsonValue,
) -> Option<Response> {
    const FIELD_PREFIX: &str = "spec.jobTemplate.spec";
    let pod_template_spec = spec
        .pointer("/jobTemplate/spec")
        .cloned()
        .unwrap_or(JsonValue::Null);
    let object_description = format!("CronJob \"{name}\"");
    if let Some(response) =
        admission::require_active_deadline_seconds(&pod_template_spec, FIELD_PREFIX)
    {
        return Some(response);
    }
    if let Some(response) = admission::validate_cronjob_schedule(&object_description, spec) {
        return Some(response);
    }
    if let Some(response) = admission::validate_cronjob_schedule_fields(&object_description, spec) {
        return Some(response);
    }
    if let Some(response) = admission::validate_ephemeral_volume_storage_classes(
        &object_description,
        &pod_template_spec,
        FIELD_PREFIX,
    ) {
        return Some(response);
    }
    if let Some(response) =
        admission::validate_resource_limits(&object_description, &pod_template_spec, FIELD_PREFIX)
    {
        return Some(response);
    }
    if let Some(response) =
        admission::validate_env_sources(&object_description, &pod_template_spec, FIELD_PREFIX)
    {
        return Some(response);
    }
    if let Some(response) =
        admission::validate_run_options(&object_description, &pod_template_spec, FIELD_PREFIX)
    {
        return Some(response);
    }
    admission::validate_plan_fits(pool, zone, &pod_template_spec).await
}

/// `spec.schedule`, kept in its own column for the reconciliation loop
/// (src/reconcile/schedule.rs) to check due-ness against.
fn schedule_of(spec: &JsonValue) -> String {
    spec.get("schedule")
        .and_then(JsonValue::as_str)
        .unwrap_or("")
        .to_string()
}

async fn cronjob_exists(pool: &SqlitePool, namespace: &str, name: &str) -> sqlx::Result<bool> {
    let row = sqlx::query("SELECT 1 FROM cronjobs WHERE name = ? AND namespace = ?")
        .bind(name)
        .bind(namespace)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

async fn load_cronjob(
    pool: &SqlitePool,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<CronJob>> {
    let row = sqlx::query(
        r#"
        SELECT id, spec, status, created_at FROM cronjobs
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(name)
    .bind(namespace)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };

    let id: String = row.get(0);
    let spec_str: String = row.get(1);
    let status_opt: Option<String> = row.get(2);
    let created_at: i64 = row.get(3);

    Ok(Some(CronJob {
        api_version: "batch/v1".to_string(),
        kind: "CronJob".to_string(),
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: Some(id),
            creation_timestamp: Some(super::secret::rfc3339(created_at)),
        },
        spec: serde_json::from_str(&spec_str)?,
        status: status_opt.and_then(|s| serde_json::from_str(&s).ok()),
    }))
}

/// JSON merge patch, RFC 7386 section 2: objects merge key by key, a
/// `null` removes a key, and anything else (lists included) replaces the
/// target's value whole.
fn merge_patch(target: &mut JsonValue, patch: &JsonValue) {
    let JsonValue::Object(patch_fields) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = JsonValue::Object(Default::default());
    }
    let target_fields = target.as_object_mut().expect("just made an object");
    for (key, value) in patch_fields {
        if value.is_null() {
            target_fields.remove(key);
        } else {
            merge_patch(
                target_fields.entry(key.clone()).or_insert(JsonValue::Null),
                value,
            );
        }
    }
}

pub async fn list_cronjobs(
    State(pool): State<SqlitePool>,
    Path(namespace): Path<String>,
) -> Result<Json<ListCronJobsResponse>, (StatusCode, String)> {
    let rows = sqlx::query(
        r#"
        SELECT id, name, namespace, spec, status FROM cronjobs
        WHERE namespace = ?
        "#,
    )
    .bind(&namespace)
    .fetch_all(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = rows
        .into_iter()
        .map(|row| {
            let id: String = row.get(0);
            let name: String = row.get(1);
            let ns: String = row.get(2);
            let spec_str: String = row.get(3);
            let status_opt: Option<String> = row.get(4);

            let spec: JsonValue = serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null);
            let status = status_opt.and_then(|s| serde_json::from_str(&s).ok());

            CronJob {
                api_version: "batch/v1".to_string(),
                kind: "CronJob".to_string(),
                metadata: ObjectMeta {
                    name,
                    namespace: Some(ns),
                    uid: Some(id),
                    creation_timestamp: None,
                },
                spec,
                status,
            }
        })
        .collect();

    Ok(Json(ListCronJobsResponse {
        api_version: "batch/v1".to_string(),
        kind: "CronJobList".to_string(),
        items,
    }))
}

pub async fn delete_cronjob(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let result = sqlx::query(
        r#"
        DELETE FROM cronjobs
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(&name)
    .bind(&namespace)
    .execute(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "CronJob not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCronJobsResponse {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<CronJob>,
}

#[cfg(test)]
mod tests {
    use super::merge_patch;
    use serde_json::json;

    #[test]
    fn test_merge_patch_rfc7386_examples() {
        // From RFC 7386, Appendix A.
        let cases = [
            (json!({"a": "b"}), json!({"a": "c"}), json!({"a": "c"})),
            (
                json!({"a": "b"}),
                json!({"b": "c"}),
                json!({"a": "b", "b": "c"}),
            ),
            (json!({"a": "b"}), json!({"a": null}), json!({})),
            (
                json!({"a": "b", "b": "c"}),
                json!({"a": null}),
                json!({"b": "c"}),
            ),
            (json!({"a": ["b"]}), json!({"a": "c"}), json!({"a": "c"})),
            (json!({"a": "c"}), json!({"a": ["b"]}), json!({"a": ["b"]})),
            (
                json!({"a": {"b": "c"}}),
                json!({"a": {"b": "d", "c": null}}),
                json!({"a": {"b": "d"}}),
            ),
            (
                json!({"a": [{"b": "c"}]}),
                json!({"a": [1]}),
                json!({"a": [1]}),
            ),
            (json!(["a", "b"]), json!(["c", "d"]), json!(["c", "d"])),
            (json!({"a": "b"}), json!(["c"]), json!(["c"])),
            (json!({"a": "foo"}), json!(null), json!(null)),
            (json!({"a": "foo"}), json!("bar"), json!("bar")),
            (
                json!({"e": null}),
                json!({"a": 1}),
                json!({"e": null, "a": 1}),
            ),
            (
                json!([1, 2]),
                json!({"a": "b", "c": null}),
                json!({"a": "b"}),
            ),
            (
                json!({}),
                json!({"a": {"bb": {"ccc": null}}}),
                json!({"a": {"bb": {}}}),
            ),
        ];
        for (mut target, patch, expected) in cases {
            merge_patch(&mut target, &patch);
            assert_eq!(target, expected, "patch {patch}");
        }
    }
}
