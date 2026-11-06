use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Secret {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub data: JsonValue,
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub secret_type: Option<String>,
}

/// What `kubectl apply`/`kubectl create secret` and OpenTofu's
/// `kubernetes_secret` send. `data` values are base64-encoded;
/// `stringData` values are plain text and get merged into `data`
/// (base64-encoded, winning over a `data` key of the same name), as in
/// Kubernetes. `type` is accepted but not stored: every Secret here is
/// reported as `Opaque`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSecretRequest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub data: Option<JsonValue>,
    #[serde(default)]
    pub string_data: Option<JsonValue>,
    #[serde(rename = "type", default)]
    pub secret_type: Option<String>,
}

const SECRET_TYPE: &str = "Opaque";

/// Merges `stringData` into `data` the way Kubernetes does: each
/// `stringData` value is base64-encoded and replaces a `data` entry of
/// the same key. Both must be objects of strings, and `data` values
/// must be valid base64 (rejected here, as Kubernetes does, rather than
/// only when a job later reads them).
fn merge_string_data(
    data: Option<JsonValue>,
    string_data: Option<JsonValue>,
) -> Result<JsonValue, String> {
    let mut merged = match data {
        None | Some(JsonValue::Null) => serde_json::Map::new(),
        Some(JsonValue::Object(map)) => map,
        Some(_) => return Err("data must be an object".to_string()),
    };
    for (key, value) in &merged {
        let JsonValue::String(encoded) = value else {
            return Err(format!("data.{key} must be a string"));
        };
        if BASE64.decode(encoded).is_err() {
            return Err(format!(
                "data.{key} is not base64-encoded (use stringData for plain values)"
            ));
        }
    }
    match string_data {
        None | Some(JsonValue::Null) => {}
        Some(JsonValue::Object(map)) => {
            for (key, value) in map {
                let JsonValue::String(plain) = value else {
                    return Err(format!("stringData.{key} must be a string"));
                };
                merged.insert(key, JsonValue::String(BASE64.encode(plain)));
            }
        }
        Some(_) => return Err("stringData must be an object".to_string()),
    }
    Ok(JsonValue::Object(merged))
}

/// RFC 3339, the format `metav1.Time` (and so client-go) parses.
pub(crate) fn rfc3339(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
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
    let data =
        merge_string_data(req.data, req.string_data).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let data_json =
        serde_json::to_string(&data).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

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
            creation_timestamp: Some(rfc3339(now)),
        },
        data,
        secret_type: Some(SECRET_TYPE.to_string()),
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
        secret_type: Some(SECRET_TYPE.to_string()),
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
                secret_type: Some(SECRET_TYPE.to_string()),
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
#[serde(rename_all = "camelCase")]
pub struct ListSecretsResponse {
    pub api_version: String,
    pub kind: String,
    pub items: Vec<Secret>,
}
