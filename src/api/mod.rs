pub mod cronjob;
pub mod secret;

#[cfg(test)]
mod tests;

use axum::{response::IntoResponse, Json};
use serde_json::json;

pub async fn discovery_v1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "v1",
        "resources": [
            {
                "name": "secrets",
                "singularName": "secret",
                "namespaced": true,
                "kind": "Secret",
                "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]
            }
        ]
    });
    Json(response)
}

pub async fn discovery_batch_v1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "batch/v1",
        "resources": [
            {
                "name": "cronjobs",
                "singularName": "cronjob",
                "namespaced": true,
                "kind": "CronJob",
                "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]
            }
        ]
    });
    Json(response)
}

pub async fn health() -> impl IntoResponse {
    Json(json!({
        "status": "healthy"
    }))
}
