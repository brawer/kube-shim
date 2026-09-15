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
pub struct ObjectMeta {
    pub name: String,
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub data: JsonValue,
    #[serde(default)]
    pub type_field: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSecretRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub data: JsonValue,
}

pub async fn create_secret(
    State(pool): State<SqlitePool>,
    Json(req): Json<CreateSecretRequest>,
) -> Result<(StatusCode, Json<Secret>), (StatusCode, String)> {
    let namespace = req
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let data_json =
        serde_json::to_string(&req.data).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    sqlx::query(
        r#"
        INSERT INTO secrets (id, name, namespace, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(&req.metadata.name)
    .bind(&namespace)
    .bind(&data_json)
    .bind(now)
    .bind(now)
    .bind(1)
    .execute(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let secret = Secret {
        api_version: "v1".to_string(),
        kind: "Secret".to_string(),
        metadata: ObjectMeta {
            name: req.metadata.name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: Some(now.to_string()),
        },
        data: req.data,
        type_field: None,
    };

    Ok((StatusCode::CREATED, Json(secret)))
}

pub async fn get_secret(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Secret>, (StatusCode, String)> {
    let row = sqlx::query(
        r#"
        SELECT id, name, namespace, data FROM secrets
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(&name)
    .bind(&namespace)
    .fetch_optional(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .ok_or_else(|| (StatusCode::NOT_FOUND, "Secret not found".to_string()))?;

    let id: String = row.get(0);
    let data_str: String = row.get(3);
    let data: JsonValue = serde_json::from_str(&data_str)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let secret = Secret {
        api_version: "v1".to_string(),
        kind: "Secret".to_string(),
        metadata: ObjectMeta {
            name,
            namespace: Some(namespace),
            uid: Some(id),
            creation_timestamp: None,
        },
        data,
        type_field: None,
    };

    Ok(Json(secret))
}

pub async fn list_secrets(
    State(pool): State<SqlitePool>,
    Path(namespace): Path<String>,
) -> Result<Json<ListSecretsResponse>, (StatusCode, String)> {
    let rows = sqlx::query(
        r#"
        SELECT id, name, namespace, data FROM secrets
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
            let data_str: String = row.get(3);
            let data: JsonValue = serde_json::from_str(&data_str).unwrap_or(JsonValue::Null);

            Secret {
                api_version: "v1".to_string(),
                kind: "Secret".to_string(),
                metadata: ObjectMeta {
                    name,
                    namespace: Some(ns),
                    uid: Some(id),
                    creation_timestamp: None,
                },
                data,
                type_field: None,
            }
        })
        .collect();

    Ok(Json(ListSecretsResponse {
        api_version: "v1".to_string(),
        kind: "SecretList".to_string(),
        items,
    }))
}

pub async fn delete_secret(
    State(pool): State<SqlitePool>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let result = sqlx::query(
        r#"
        DELETE FROM secrets
        WHERE name = ? AND namespace = ?
        "#,
    )
    .bind(&name)
    .bind(&namespace)
    .execute(&pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "Secret not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
pub struct ListSecretsResponse {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Secret>,
}
