//! Secret updates through the real router (issue #106): JSON Patch
//! (RFC 6902), which is how the `hashicorp/kubernetes` provider changes
//! a Secret (`resourceKubernetesSecretV1Update`).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use kube_shim::config::ApiToken;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "test-token";
const SECRETS: &str = "/api/v1/namespaces/default/secrets";
const ITEM: &str = "/api/v1/namespaces/default/secrets/s3";
const JSON_PATCH: &str = "application/json-patch+json";

async fn test_router() -> axum::Router {
    let pool = kube_shim::db::init_pool(":memory:").await.unwrap();
    kube_shim::app::build_router(
        pool,
        Arc::new(vec![ApiToken {
            token: TOKEN.to_string(),
            expires_at: None,
        }]),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(kube_shim::ssh::WorkerSshConfig {
            private_key: String::new(),
            port: kube_shim::ssh::SSH_PORT,
        }),
        Arc::new(kube_shim::api::cost_report::CostReportConfig {
            resource_prefix: "kube-shim-test".to_string(),
            zone: "de-fra1".to_string(),
            main_currency: "EUR".to_string(),
            provider_name: "UpCloud".to_string(),
            invoice_issuer_name: "UpCloud Ltd".to_string(),
        }),
        Arc::new(kube_shim::reconcile::TickHealth::new()),
    )
}

fn b64(s: &str) -> String {
    BASE64.encode(s)
}

fn request(method: &str, uri: &str, content_type: &str, body: String) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", content_type)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body))
        .unwrap()
}

async fn send(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
    (status, json)
}

/// A router holding Secret "s3" with `{"A": "1", "B": "2"}`.
async fn router_with_secret() -> axum::Router {
    let router = test_router().await;
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "s3", "namespace": "default"},
        "data": {"A": b64("1"), "B": b64("2")},
    });
    let (status, body) = send(
        &router,
        request("POST", SECRETS, "application/json", body.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    router
}

async fn patch(router: &axum::Router, ops: Value) -> (StatusCode, Value) {
    send(router, request("PATCH", ITEM, JSON_PATCH, ops.to_string())).await
}

async fn get_data(router: &axum::Router) -> Value {
    let get = Request::builder()
        .uri(ITEM)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(router, get).await;
    assert_eq!(status, StatusCode::OK);
    body["data"].clone()
}

/// The provider replaces the whole `data` map with one `add`.
#[tokio::test]
async fn test_provider_data_update() {
    let router = router_with_secret().await;
    let new_data = json!({"A": b64("one"), "C": b64("3")});
    let (status, body) = patch(
        &router,
        json!([{"op": "add", "path": "/data", "value": new_data}]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"], new_data);
    assert_eq!(get_data(&router).await, new_data);
}

#[tokio::test]
async fn test_single_key_ops() {
    let router = router_with_secret().await;
    let (status, body) = patch(
        &router,
        json!([
            {"op": "test", "path": "/data/A", "value": b64("1")},
            {"op": "replace", "path": "/data/A", "value": b64("x")},
            {"op": "remove", "path": "/data/B"},
            {"op": "add", "path": "/stringData", "value": {"D": "plain"}},
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        get_data(&router).await,
        json!({"A": b64("x"), "D": b64("plain")})
    );
}

/// A failing op (here a `test`) leaves the Secret as it was.
#[tokio::test]
async fn test_failed_patch_changes_nothing() {
    let router = router_with_secret().await;
    let (status, body) = patch(
        &router,
        json!([
            {"op": "replace", "path": "/data/A", "value": b64("x")},
            {"op": "test", "path": "/data/B", "value": b64("wrong")},
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["kind"], "Status");
    assert_eq!(
        get_data(&router).await,
        json!({"A": b64("1"), "B": b64("2")})
    );
}

/// kube-shim doesn't store labels: refused rather than dropped, so a
/// later `tofu plan` doesn't show the same change forever.
#[tokio::test]
async fn test_labels_refused() {
    let router = router_with_secret().await;
    let (status, body) = patch(
        &router,
        json!([{"op": "add", "path": "/metadata/labels", "value": {"app": "osmdiffs"}}]),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("metadata.labels"),
        "{body}"
    );
    // An empty map stores nothing, so it's fine.
    let (status, body) = patch(
        &router,
        json!([{"op": "add", "path": "/metadata/labels", "value": {}}]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn test_unsupported_changes_refused() {
    let router = router_with_secret().await;
    for ops in [
        json!([{"op": "replace", "path": "/immutable", "value": true}]),
        json!([{"op": "replace", "path": "/type", "value": "kubernetes.io/tls"}]),
        json!([{"op": "replace", "path": "/metadata/name", "value": "other"}]),
        json!([{"op": "replace", "path": "/kind", "value": "ConfigMap"}]),
        json!([{"op": "add", "path": "/foo", "value": 1}]),
        json!([{"op": "replace", "path": "/data/A", "value": "not base64!"}]),
    ] {
        let (status, body) = patch(&router, ops.clone()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{ops}: {body}");
    }
    // `immutable: false` is what kube-shim does anyway.
    let (status, body) = patch(
        &router,
        json!([{"op": "add", "path": "/immutable", "value": false}]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        get_data(&router).await,
        json!({"A": b64("1"), "B": b64("2")})
    );
}

#[tokio::test]
async fn test_other_patch_types_are_415() {
    let router = router_with_secret().await;
    for content_type in [
        "application/merge-patch+json",
        "application/strategic-merge-patch+json",
        "application/json",
    ] {
        let (status, body) = send(
            &router,
            request("PATCH", ITEM, content_type, json!({"data": {}}).to_string()),
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{content_type}");
        assert_eq!(body["reason"], "UnsupportedMediaType");
    }
}

#[tokio::test]
async fn test_patch_missing_secret_is_404() {
    let router = test_router().await;
    let (status, body) = patch(&router, json!([])).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["reason"], "NotFound");
}

#[tokio::test]
async fn test_malformed_patch_is_400() {
    let router = router_with_secret().await;
    let (status, body) = patch(&router, json!({"op": "add"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}
