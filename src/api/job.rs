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

use super::cost_report::CostReportConfig;
use super::pods::phase_for;
use super::secret::{rfc3339, ObjectMeta, OwnerReference};
use crate::reconcile::history;
use crate::reconcile::job as reconcile_job;
use crate::reconcile::job::RUN_FAILED_SQL;
use crate::{admission, k8s_status, pricing};
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
use std::collections::BTreeMap;
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub active: i32,
    pub succeeded: i32,
    pub failed: i32,
    pub conditions: Vec<JobCondition>,
    /// When the Job started: its creation, since kube-shim starts every
    /// Job right away (there's no `suspend` for Jobs here).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    /// When the Job completed. Like in Kubernetes, only set for a Job
    /// that succeeded, never for one that failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJobRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobList {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Job>,
}

/// Mirrors `api::pods::phase_for`'s own state->outcome inference, in
/// `JobStatus`'s own `active`/`succeeded`/`failed`/`conditions` shape
/// instead of `Pod`'s single `phase` string -- one inference, two
/// representations, not a second copy of this same three-way mapping.
fn status_for(status: &str, failed: bool) -> JobStatus {
    match phase_for(status, failed) {
        "Succeeded" => JobStatus {
            active: 0,
            succeeded: 1,
            failed: 0,
            conditions: vec![JobCondition {
                condition_type: "Complete".to_string(),
                status: "True".to_string(),
            }],
            start_time: None,
            completion_time: None,
        },
        "Failed" => JobStatus {
            active: 0,
            succeeded: 0,
            failed: 1,
            conditions: vec![JobCondition {
                condition_type: "Failed".to_string(),
                status: "True".to_string(),
            }],
            start_time: None,
            completion_time: None,
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
            start_time: None,
            completion_time: None,
        },
    }
}

/// Labels the API server adds to a Job's pod template, and selects in
/// `spec.selector`, unless `spec.manualSelector` is set: upstream's
/// `generateSelector` in `pkg/registry/batch/job/strategy.go`. Both the
/// current `batch.kubernetes.io/` keys and the legacy unprefixed ones.
const JOB_NAME_LABELS: [&str; 2] = ["job-name", "batch.kubernetes.io/job-name"];
const CONTROLLER_UID_LABELS: [&str; 2] = ["controller-uid", "batch.kubernetes.io/controller-uid"];

/// What a CronJob's controller sets on each Job it creates, to the time
/// it was scheduled for (upstream's `getJobFromTemplate2`).
pub(crate) const CRONJOB_SCHEDULED_TIMESTAMP_ANNOTATION: &str =
    "batch.kubernetes.io/cronjob-scheduled-timestamp";

/// The columns `job_from_row` reads, in its order. The owning CronJob
/// is looked up by the `cronjob_uid` recorded when the run was created,
/// or by name for runs from before that column; either way only while
/// it still exists, since deleting a CronJob here orphans its runs, and
/// Kubernetes removes the owner reference from an orphaned Job.
const JOB_COLUMNS: &str = "jobs.id, jobs.name, jobs.namespace, jobs.spec, jobs.status, \
     {RUN_FAILED_SQL}, jobs.created_at, jobs.completed_at, jobs.labels, jobs.annotations, \
     jobs.cronjob_name, cronjobs.id";
const JOB_FROM: &str = "jobs LEFT JOIN cronjobs ON cronjobs.namespace = jobs.namespace \
     AND cronjobs.name = jobs.cronjob_name \
     AND (jobs.cronjob_uid IS NULL OR cronjobs.id = jobs.cronjob_uid)";

fn select_jobs_sql(condition: &str) -> String {
    let columns = JOB_COLUMNS.replace("{RUN_FAILED_SQL}", RUN_FAILED_SQL);
    format!("SELECT {columns} FROM {JOB_FROM} WHERE {condition}")
}

/// A stored `labels`/`annotations` column: a JSON object of strings, or
/// NULL for none.
pub(crate) fn parse_string_map(column: Option<String>) -> BTreeMap<String, String> {
    column
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// A `labels`/`annotations` map as stored: NULL when empty.
pub(crate) fn string_map_column(map: Option<&BTreeMap<String, String>>) -> Option<String> {
    map.filter(|m| !m.is_empty())
        .map(|m| serde_json::to_string(m).expect("a string map serializes"))
}

/// Everything about a Job that `job_from_parts` needs, as stored.
struct StoredJob {
    id: String,
    name: String,
    namespace: String,
    spec: JsonValue,
    status: String,
    failed: bool,
    created_at: i64,
    completed_at: Option<i64>,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    cronjob_name: Option<String>,
    cronjob_uid: Option<String>,
}

fn row_to_job(row: SqliteRow) -> Job {
    let spec_str: String = row.get(3);
    job_from_parts(StoredJob {
        id: row.get(0),
        name: row.get(1),
        namespace: row.get(2),
        spec: serde_json::from_str(&spec_str).unwrap_or(JsonValue::Null),
        status: row.get(4),
        failed: row.get(5),
        created_at: row.get(6),
        completed_at: row.get(7),
        labels: parse_string_map(row.get(8)),
        annotations: parse_string_map(row.get(9)),
        cronjob_name: row.get(10),
        cronjob_uid: row.get(11),
    })
}

/// A Job as the Kubernetes API server would return it, to the extent
/// kube-shim knows. The API server defaults a few things when it stores
/// a Job, which kube-shim does when it returns one instead, so they
/// apply to Jobs created before this, too:
///
/// - `spec.selector` and the pod template's labels get `job-name` and
///   `controller-uid` labels (`generateSelector`), unless
///   `spec.manualSelector` is set;
/// - `metadata.labels` default to the pod template's labels when the
///   Job has none of its own (`SetDefaults_Job`);
/// - a CronJob's run is owned by it and annotated with the time it was
///   scheduled for (`getJobFromTemplate2`).
fn job_from_parts(stored: StoredJob) -> Job {
    let StoredJob {
        id,
        name,
        namespace,
        mut spec,
        status,
        failed,
        created_at,
        completed_at,
        mut labels,
        mut annotations,
        cronjob_name,
        cronjob_uid,
    } = stored;

    let manual_selector = spec.get("manualSelector").and_then(JsonValue::as_bool) == Some(true);
    if !manual_selector && spec.is_object() {
        generate_selector(&mut spec, &name, &id);
    }
    if labels.is_empty() {
        if let Some(template_labels) = spec
            .pointer("/template/metadata/labels")
            .and_then(JsonValue::as_object)
        {
            labels = template_labels
                .iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect();
        }
    }

    let mut owner_references = None;
    if let Some(cronjob_name) = cronjob_name {
        annotations
            .entry(CRONJOB_SCHEDULED_TIMESTAMP_ANNOTATION.to_string())
            .or_insert_with(|| rfc3339(created_at));
        if let Some(uid) = cronjob_uid {
            owner_references = Some(vec![OwnerReference {
                api_version: "batch/v1".to_string(),
                kind: "CronJob".to_string(),
                name: cronjob_name,
                uid,
                controller: Some(true),
                block_owner_deletion: Some(true),
            }]);
        }
    }

    let mut job_status = status_for(&status, failed);
    job_status.start_time = Some(rfc3339(created_at));
    if job_status.succeeded > 0 {
        job_status.completion_time = completed_at.map(rfc3339);
    }

    Job {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        metadata: ObjectMeta {
            name,
            namespace: Some(namespace),
            uid: Some(id),
            // Jobs can't be changed here, so their spec never moves on
            // from its first generation.
            generation: Some(1),
            creation_timestamp: Some(rfc3339(created_at)),
            labels: (!labels.is_empty()).then_some(labels),
            annotations: (!annotations.is_empty()).then_some(annotations),
            owner_references,
        },
        spec,
        status: job_status,
    }
}

/// Upstream's `generateSelector`: labels the pod template with the
/// Job's name and UID, keeping any value the user already gave for one
/// of those keys, and selects pods by UID.
fn generate_selector(spec: &mut JsonValue, name: &str, uid: &str) {
    let template_labels = object_at(spec, &["template", "metadata", "labels"]);
    for key in JOB_NAME_LABELS {
        template_labels
            .entry(key)
            .or_insert_with(|| JsonValue::String(name.to_string()));
    }
    for key in CONTROLLER_UID_LABELS {
        template_labels
            .entry(key)
            .or_insert_with(|| JsonValue::String(uid.to_string()));
    }
    object_at(spec, &["selector", "matchLabels"])
        .entry("batch.kubernetes.io/controller-uid")
        .or_insert_with(|| JsonValue::String(uid.to_string()));
}

/// The object at `path` under `value` (an object), creating it and any
/// object on the way, and replacing anything there that isn't one.
fn object_at<'a>(
    value: &'a mut JsonValue,
    path: &[&str],
) -> &'a mut serde_json::Map<String, JsonValue> {
    let mut current = value;
    for key in path {
        let map = current.as_object_mut().expect("checked or just made");
        let next = map
            .entry(*key)
            .or_insert_with(|| JsonValue::Object(Default::default()));
        if !next.is_object() {
            *next = JsonValue::Object(Default::default());
        }
        current = next;
    }
    current.as_object_mut().expect("just made an object")
}

pub async fn create_job(
    State(pool): State<SqlitePool>,
    Extension(notify): Extension<Arc<Notify>>,
    Extension(cost_report_config): Extension<Arc<CostReportConfig>>,
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
    if let Some(response) =
        admission::validate_env_sources(&object_description, &req.spec, FIELD_PREFIX)
    {
        return response;
    }
    if let Some(response) =
        admission::validate_run_options(&object_description, &req.spec, FIELD_PREFIX)
    {
        return response;
    }
    if let Some(response) =
        admission::validate_plan_fits(&pool, &cost_report_config.zone, &req.spec).await
    {
        return response;
    }

    // Budget admission (Phase 14b, moved here from a `BudgetWait`
    // state that created the row first and found out later): rejected
    // synchronously, the same way a real cluster's `ResourceQuota`
    // admission controller would, rather than creating a row that
    // might just sit idle. `require_active_deadline_seconds` above
    // already guarantees this unwrap succeeds.
    let deadline = reconcile_job::active_deadline_seconds(&req.spec).unwrap();
    let main_currency =
        match pricing::effective_main_currency(&pool, &cost_report_config.main_currency).await {
            Ok(c) => c,
            Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };
    let (estimated_cost, sufficient) = match pricing::check_budget(
        &pool,
        &cost_report_config.zone,
        &req.spec,
        deadline,
        &main_currency,
    )
    .await
    {
        Ok(result) => result,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    if !sufficient {
        return k8s_status::policy_denied(
            "kube-shim.brawer.ch/require-sufficient-budget",
            format!(
                "estimated cost ({:.4} {main_currency}) exceeds the current budget balance \
                 -- top up via POST /apis/cost.kube-shim.brawer.ch/v1/budget/topup and \
                 resubmit",
                estimated_cost.unwrap_or(0.0)
            ),
        );
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
    // estimated_cost was already computed above, at admission time --
    // handle_created no longer exists to compute it again on the
    // first tick.
    let labels = req.metadata.labels.unwrap_or_default();
    let annotations = req.metadata.annotations.unwrap_or_default();
    let insert_result = sqlx::query(
        "INSERT INTO jobs (id, name, namespace, spec, status, estimated_cost, labels, annotations, \
         created_at, updated_at, version) \
         VALUES (?, ?, ?, ?, 'Created', ?, ?, ?, ?, ?, 1)",
    )
    .bind(&id)
    .bind(&req.metadata.name)
    .bind(&namespace)
    .bind(&spec_json)
    .bind(estimated_cost)
    .bind(string_map_column(Some(&labels)))
    .bind(string_map_column(Some(&annotations)))
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

    let job = job_from_parts(StoredJob {
        id,
        name: req.metadata.name,
        namespace,
        spec: req.spec,
        status: "Created".to_string(),
        failed: false,
        created_at: now,
        completed_at: None,
        labels,
        annotations,
        cronjob_name: None,
        cronjob_uid: None,
    });

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
    let rows = sqlx::query(&select_jobs_sql("jobs.namespace = ? ORDER BY jobs.name"))
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
    let row = sqlx::query(&select_jobs_sql("jobs.namespace = ? AND jobs.name = ?"))
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
/// `api::cronjob::delete_cronjob`'s own immediate-removal behavior --
/// through `reconcile::history::prune_job`, so its cost stays in the
/// cost report.
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
        history::prune_job(&pool, &id, Utc::now().timestamp())
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
            .layer(Extension(Arc::new(CostReportConfig {
                resource_prefix: "kube-shim-test".to_string(),
                zone: "de-fra1".to_string(),
                main_currency: "EUR".to_string(),
                provider_name: "UpCloud".to_string(),
                invoice_issuer_name: "UpCloud Ltd".to_string(),
            })))
            .with_state(pool)
    }

    fn create_request(name: &str, spec: JsonValue) -> axum::http::Request<axum::body::Body> {
        let body = json!({
            "apiVersion": "batch/v1",
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

    async fn insert_price(pool: &SqlitePool) {
        sqlx::query(
            "INSERT INTO provider_pricing (zone, price_key, amount, price, currency, fetched_at) \
             VALUES ('de-fra1', 'server_plan_DEV-1xCPU-1GB-10GB', 1.0, 0.4464, 'EUR', 0)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_budget(pool: &SqlitePool, balance: f64, daily_rate: f64) {
        sqlx::query(
            "INSERT INTO budget_state \
             (id, balance, last_accrual_at, main_currency, budget_daily_rate, budget_rollover_cap_days) \
             VALUES (1, ?, ?, 'EUR', ?, 7)",
        )
        .bind(balance)
        .bind(Utc::now().timestamp())
        .bind(daily_rate)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_create_job_rejected_at_admission_when_budget_insufficient() {
        // The real simplification over this project's first attempt at
        // the budget guard (a BudgetWait state the job sat in, created
        // anyway): reject synchronously, before any row exists at all,
        // the same way a real cluster's ResourceQuota admission
        // controller would.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool).await;
        seed_budget(&pool, 0.0, 2.0).await;

        let response = router(pool.clone())
            .oneshot(create_request("too-expensive", valid_spec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = body_json(response).await;
        assert_eq!(body["reason"], "Forbidden");
        let message = body["message"].as_str().unwrap();
        assert!(message.contains("require-sufficient-budget"));
        assert!(message.contains("budget/topup"));

        // Nothing was created at all -- not even a row to hold a
        // rejection reason on.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    fn spec_requesting(cpu: &str, memory: &str) -> JsonValue {
        let mut spec = valid_spec();
        spec["template"]["spec"]["containers"][0]["resources"] =
            json!({"requests": {"cpu": cpu, "memory": memory}});
        spec
    }

    #[tokio::test]
    async fn test_create_job_rejected_at_admission_when_no_plan_fits() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;

        let response = router(pool.clone())
            .oneshot(create_request("too-big", spec_requesting("6", "8Gi")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let message = body_json(response).await["message"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(message.contains("require-fitting-server-plan"));
        assert!(message.contains("6 CPU / 8 GB"));
    }

    #[tokio::test]
    async fn test_create_job_accepted_while_plan_catalog_not_synced_yet() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let response = router(pool.clone())
            .oneshot(create_request("early", spec_requesting("6", "8Gi")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_create_job_accepted_and_stores_estimated_cost_when_budget_covers_it() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        crate::workload::seed_test_plans(&pool).await;
        insert_price(&pool).await;
        seed_budget(&pool, 10.0, 2.0).await;

        let response = router(pool.clone())
            .oneshot(create_request("affordable", valid_spec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let estimated_cost: Option<f64> =
            sqlx::query_scalar("SELECT estimated_cost FROM jobs WHERE name = 'affordable'")
                .fetch_one(&pool)
                .await
                .unwrap();
        // activeDeadlineSeconds: 300 (5 minutes) at the DEV plan's own
        // cents/hour rate.
        assert!((estimated_cost.unwrap() - 0.0003720).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_create_job_accepted_when_budgeting_disabled() {
        // No budget_state row at all -- the default for every
        // already-deployed instance that's never configured a budget.
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_price(&pool).await;

        let response = router(pool)
            .oneshot(create_request("no-budget-configured", valid_spec()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
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
    async fn test_archived_job_with_nonzero_exit_code_reports_failed() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_job_with_status(&pool, "failed-one", "Archived").await;
        sqlx::query("UPDATE jobs SET exit_code = 1 WHERE name = 'failed-one'")
            .execute(&pool)
            .await
            .unwrap();

        let response = router(pool)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/apis/batch/v1/namespaces/default/jobs/failed-one")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = body_json(response).await;
        assert_eq!(body["status"]["failed"], 1);
        assert_eq!(body["status"]["succeeded"], 0);
        assert_eq!(body["status"]["conditions"][0]["type"], "Failed");
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

    async fn get_json(pool: &SqlitePool, uri: &str) -> JsonValue {
        let response = router(pool.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        body_json(response).await
    }

    /// Issue #154: `kubectl get jobs --sort-by=.metadata.creationTimestamp`
    /// failed, and AGE was `<unknown>`, because a listed Job had no
    /// `creationTimestamp`.
    #[tokio::test]
    async fn test_listed_job_has_the_metadata_kubernetes_would_return() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let mut spec = valid_spec();
        spec["template"]["metadata"] = json!({"labels": {"app": "osmdiffs"}});
        let created = body_json(
            router(pool.clone())
                .oneshot(create_request("manual-run", spec))
                .await
                .unwrap(),
        )
        .await;
        let uid = created["metadata"]["uid"].as_str().unwrap().to_string();
        let created_at: i64 = sqlx::query_scalar("SELECT created_at FROM jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        let timestamp = rfc3339(created_at);

        let list = get_json(&pool, "/apis/batch/v1/namespaces/default/jobs").await;
        let job = &list["items"][0];
        assert_eq!(job, &created, "create and list return the same Job");
        let expected_labels = json!({
            "app": "osmdiffs",
            "batch.kubernetes.io/controller-uid": uid,
            "batch.kubernetes.io/job-name": "manual-run",
            "controller-uid": uid,
            "job-name": "manual-run",
        });
        assert_eq!(
            job["metadata"],
            json!({
                "name": "manual-run",
                "namespace": "default",
                "uid": uid,
                "generation": 1,
                "creationTimestamp": timestamp,
                "labels": expected_labels,
            })
        );
        assert_eq!(
            job["spec"]["template"]["metadata"]["labels"],
            expected_labels
        );
        assert_eq!(
            job["spec"]["selector"],
            json!({"matchLabels": {"batch.kubernetes.io/controller-uid": uid}})
        );
        assert_eq!(job["status"]["startTime"], timestamp);
        assert!(job["status"].get("completionTime").is_none());

        let got = get_json(&pool, "/apis/batch/v1/namespaces/default/jobs/manual-run").await;
        assert_eq!(got, created);
    }

    /// Labels and annotations given for the Job itself are kept, and
    /// then the pod template's labels aren't copied to it.
    #[tokio::test]
    async fn test_job_keeps_its_own_labels_and_annotations() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let body = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "labelled",
                "labels": {"team": "osm"},
                "annotations": {"note": "testing v0.4.3"},
            },
            "spec": valid_spec(),
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/apis/batch/v1/namespaces/default/jobs")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let response = router(pool.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let job = get_json(&pool, "/apis/batch/v1/namespaces/default/jobs/labelled").await;
        assert_eq!(job["metadata"]["labels"], json!({"team": "osm"}));
        assert_eq!(
            job["metadata"]["annotations"],
            json!({"note": "testing v0.4.3"})
        );
        assert_eq!(
            job["spec"]["template"]["metadata"]["labels"]["job-name"],
            "labelled"
        );
    }

    #[tokio::test]
    async fn test_manual_selector_job_gets_no_generated_labels() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        let mut spec = valid_spec();
        spec["manualSelector"] = json!(true);
        spec["selector"] = json!({"matchLabels": {"app": "x"}});
        spec["template"]["metadata"] = json!({"labels": {"app": "x"}});
        let response = router(pool.clone())
            .oneshot(create_request("manual-selector", spec.clone()))
            .await
            .unwrap();
        let job = body_json(response).await;
        assert_eq!(job["spec"], spec);
        assert_eq!(job["metadata"]["labels"], json!({"app": "x"}));
    }

    async fn insert_cronjob(pool: &SqlitePool, id: &str, name: &str) {
        sqlx::query(
            "INSERT INTO cronjobs (id, name, namespace, spec, schedule, created_at, updated_at) \
             VALUES (?, ?, 'default', '{}', '* * * * *', 0, 0)",
        )
        .bind(id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_cronjob_run(pool: &SqlitePool, name: &str, cronjob_uid: Option<&str>) {
        sqlx::query(
            "INSERT INTO jobs (id, name, namespace, cronjob_name, cronjob_uid, spec, status, \
             labels, created_at, completed_at, updated_at, version) \
             VALUES (?, ?, 'default', 'weekly', ?, '{}', 'Archived', '{\"app\":\"osmdiffs\"}', \
             1794711605, 1794715205, 1794715205, 1)",
        )
        .bind(format!("{name}-uid"))
        .bind(name)
        .bind(cronjob_uid)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cronjob_run_is_owned_by_its_cronjob() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "cronjob-uid", "weekly").await;
        insert_cronjob_run(&pool, "weekly-1794711605", Some("cronjob-uid")).await;

        let job = get_json(
            &pool,
            "/apis/batch/v1/namespaces/default/jobs/weekly-1794711605",
        )
        .await;
        assert_eq!(
            job["metadata"]["ownerReferences"],
            json!([{
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "name": "weekly",
                "uid": "cronjob-uid",
                "controller": true,
                "blockOwnerDeletion": true,
            }])
        );
        assert_eq!(
            job["metadata"]["annotations"],
            json!({"batch.kubernetes.io/cronjob-scheduled-timestamp": "2026-11-15T03:00:05Z"})
        );
        assert_eq!(job["metadata"]["labels"], json!({"app": "osmdiffs"}));
        assert_eq!(job["metadata"]["creationTimestamp"], "2026-11-15T03:00:05Z");
        assert_eq!(job["status"]["startTime"], "2026-11-15T03:00:05Z");
        assert_eq!(job["status"]["completionTime"], "2026-11-15T04:00:05Z");
    }

    /// Deleting a CronJob here leaves its runs behind, which Kubernetes
    /// calls orphaning: their owner reference goes away. A CronJob
    /// created again under the same name doesn't own them either.
    #[tokio::test]
    async fn test_orphaned_cronjob_run_has_no_owner() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "new-cronjob-uid", "weekly").await;
        insert_cronjob_run(&pool, "orphan", Some("deleted-cronjob-uid")).await;
        let job = get_json(&pool, "/apis/batch/v1/namespaces/default/jobs/orphan").await;
        assert!(job["metadata"].get("ownerReferences").is_none(), "{job}");
        assert!(job["metadata"]["annotations"]
            .get("batch.kubernetes.io/cronjob-scheduled-timestamp")
            .is_some());
    }

    /// Runs from before `jobs.cronjob_uid` find their CronJob by name.
    #[tokio::test]
    async fn test_older_cronjob_run_finds_its_owner_by_name() {
        let pool = crate::db::init_pool(":memory:").await.unwrap();
        insert_cronjob(&pool, "cronjob-uid", "weekly").await;
        insert_cronjob_run(&pool, "older", None).await;
        let job = get_json(&pool, "/apis/batch/v1/namespaces/default/jobs/older").await;
        assert_eq!(job["metadata"]["ownerReferences"][0]["uid"], "cronjob-uid");
    }
}
