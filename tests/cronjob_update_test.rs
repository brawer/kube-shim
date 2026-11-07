//! CronJob updates through the real router (issue #105): `PUT` (what the
//! `hashicorp/kubernetes` provider sends for any in-place change, e.g.
//! flipping `suspend`), JSON merge `PATCH`, and per-namespace name
//! uniqueness on create.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kube_shim::config::ApiToken;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "test-token";
const ITEM_URI: &str = "/apis/batch/v1/namespaces/default/cronjobs/osmdiffs-weekly";

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

/// Roughly what the provider sends for production's osmdiffs CronJob.
fn osmdiffs_spec(suspend: bool) -> Value {
    json!({
        "schedule": "0 3 * * 0",
        "suspend": suspend,
        "concurrencyPolicy": "Forbid",
        "startingDeadlineSeconds": 3600,
        "successfulJobsHistoryLimit": 3,
        "failedJobsHistoryLimit": 3,
        "jobTemplate": {"spec": {
            "activeDeadlineSeconds": 57600,
            "template": {"spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "osmdiffs", "image": "ghcr.io/brawer/osmdiffs:latest"}]
            }}
        }}
    })
}

fn cronjob_body(name: &str, spec: Value) -> String {
    json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    })
    .to_string()
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

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap()
}

async fn create(router: &axum::Router, name: &str, spec: Value) -> StatusCode {
    router
        .clone()
        .oneshot(request(
            "POST",
            "/apis/batch/v1/namespaces/default/cronjobs",
            "application/json",
            cronjob_body(name, spec),
        ))
        .await
        .unwrap()
        .status()
}

async fn send(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn test_create_with_an_existing_name_conflicts() {
    let router = test_router().await;
    assert_eq!(
        create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await,
        StatusCode::CREATED
    );
    assert_eq!(
        create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn test_put_unsuspends_and_get_returns_what_was_sent() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;
    let (_, created) = send(&router, get(ITEM_URI)).await;

    let (status, updated) = send(
        &router,
        request(
            "PUT",
            ITEM_URI,
            "application/json",
            cronjob_body("osmdiffs-weekly", osmdiffs_spec(false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["spec"], osmdiffs_spec(false));
    assert_eq!(updated["metadata"]["uid"], created["metadata"]["uid"]);
    assert_eq!(
        updated["metadata"]["creationTimestamp"],
        created["metadata"]["creationTimestamp"]
    );

    // No drift for `tofu plan`: GET returns the spec exactly as sent.
    let (status, fetched) = send(&router, get(ITEM_URI)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["spec"], osmdiffs_spec(false));
}

#[tokio::test]
async fn test_put_runs_admission_checks() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;

    let mut spec = osmdiffs_spec(false);
    spec["concurrencyPolicy"] = json!("Replace");
    let (status, body) = send(
        &router,
        request(
            "PUT",
            ITEM_URI,
            "application/json",
            cronjob_body("osmdiffs-weekly", spec),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("spec.concurrencyPolicy"));

    // Rejected updates change nothing.
    let (_, fetched) = send(&router, get(ITEM_URI)).await;
    assert_eq!(fetched["spec"], osmdiffs_spec(true));
}

#[tokio::test]
async fn test_put_of_a_missing_cronjob_is_not_found() {
    let router = test_router().await;
    let (status, body) = send(
        &router,
        request(
            "PUT",
            ITEM_URI,
            "application/json",
            cronjob_body("osmdiffs-weekly", osmdiffs_spec(false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["reason"], "NotFound");
}

#[tokio::test]
async fn test_put_with_a_different_name_is_rejected() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;
    let (status, body) = send(
        &router,
        request(
            "PUT",
            ITEM_URI,
            "application/json",
            cronjob_body("something-else", osmdiffs_spec(false)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["reason"], "BadRequest");
}

#[tokio::test]
async fn test_merge_patch_flips_suspend_only() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;

    let (status, patched) = send(
        &router,
        request(
            "PATCH",
            ITEM_URI,
            "application/merge-patch+json",
            json!({"spec": {"suspend": false}}).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patched["spec"], osmdiffs_spec(false));
}

#[tokio::test]
async fn test_strategic_merge_patch_is_refused_with_a_hint() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;

    let (status, body) = send(
        &router,
        request(
            "PATCH",
            ITEM_URI,
            "application/strategic-merge-patch+json",
            json!({"spec": {"suspend": false}}).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert!(body["message"].as_str().unwrap().contains("--type merge"));

    let (_, fetched) = send(&router, get(ITEM_URI)).await;
    assert_eq!(fetched["spec"], osmdiffs_spec(true));
}

#[tokio::test]
async fn test_patch_cannot_rename() {
    let router = test_router().await;
    create(&router, "osmdiffs-weekly", osmdiffs_spec(true)).await;

    let (status, _) = send(
        &router,
        request(
            "PATCH",
            ITEM_URI,
            "application/merge-patch+json",
            json!({"metadata": {"name": "other"}}).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
