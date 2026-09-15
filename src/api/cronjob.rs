use super::secret::ObjectMeta;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCronJobRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: JsonValue,
}

pub async fn create_cronjob(
    State(pool): State<SqlitePool>,
    Json(req): Json<CreateCronJobRequest>,
) -> Result<(StatusCode, Json<CronJob>), (StatusCode, String)> {
    let namespace = req
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let spec_json =
        serde_json::to_string(&req.spec).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    sqlx::query(
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
    .bind("") // schedule will be extracted from spec in Phase 2
    .bind(1)
    .execute(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let cronjob = CronJob {
        api_version: "batch/v1".to_string(),
        kind: "CronJob".to_string(),
        metadata: ObjectMeta {
            name: req.metadata.name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: Some(now.to_string()),
        },
        spec: req.spec,
        status: None,
    };

    Ok((StatusCode::CREATED, Json(cronjob)))
}

pub async fn get_cronjob(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<CronJob>, (StatusCode, String)> {
    let row = sqlx::query(
        r#"
        SELECT id, name, namespace, spec, status FROM cronjobs
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(&name)
    .bind(&namespace)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .ok_or_else(|| (StatusCode::NOT_FOUND, "CronJob not found".to_string()))?;

    let id: String = row.get(0);
    let spec_str: String = row.get(3);
    let status_opt: Option<String> = row.get(4);

    let spec: JsonValue = serde_json::from_str(&spec_str)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let status = status_opt.and_then(|s| serde_json::from_str(&s).ok());

    let cronjob = CronJob {
        api_version: "batch/v1".to_string(),
        kind: "CronJob".to_string(),
        metadata: ObjectMeta {
            name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: None,
        },
        spec,
        status,
    };

    Ok(Json(cronjob))
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
pub struct ListCronJobsResponse {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<CronJob>,
}
