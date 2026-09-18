//! Exercises Phase 5's CronJob admission checks through the real router
//! (same construction main.rs uses), not just the underlying functions
//! directly -- confirms the checks are actually wired into the HTTP path,
//! not just correct in isolation.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kube_shim::config::ApiToken;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "test-token";

async fn test_router() -> axum::Router {
    let pool = kube_shim::db::init_pool(":memory:").await.unwrap();
    kube_shim::app::build_router(
        pool,
        Arc::new(vec![ApiToken {
            token: TOKEN.to_string(),
            expires_at: None,
        }]),
        Arc::new(tokio::sync::Notify::new()),
    )
}

fn create_cronjob_request(name: &str, spec: Value) -> Request<Body> {
    let body = json!({
        "api_version": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    });
    Request::builder()
        .method("POST")
        .uri("/apis/batch/v1/namespaces/default/cronjobs")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn test_cronjob_without_active_deadline_seconds_rejected() {
    let router = test_router().await;
    let spec = json!({
        "schedule": "0 2 * * 0",
        "jobTemplate": {"spec": {"template": {"spec": {
            "containers": [{"name": "x", "image": "y"}]
        }}}}
    });

    let response = router
        .oneshot(create_cronjob_request("no-deadline", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let json = response_json(response).await;
    assert_eq!(json["kind"], "Status");
    assert_eq!(json["reason"], "Forbidden");
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("activeDeadlineSeconds"));
}

#[tokio::test]
async fn test_cronjob_with_active_deadline_seconds_accepted() {
    let router = test_router().await;
    let spec = json!({
        "schedule": "0 2 * * 0",
        "jobTemplate": {"spec": {
            "activeDeadlineSeconds": 3600,
            "template": {"spec": {"containers": [{"name": "x", "image": "y"}]}}
        }}
    });

    let response = router
        .oneshot(create_cronjob_request("with-deadline", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_cronjob_with_unknown_storage_class_rejected() {
    let router = test_router().await;
    let spec = json!({
        "schedule": "0 2 * * 0",
        "jobTemplate": {"spec": {
            "activeDeadlineSeconds": 3600,
            "template": {"spec": {
                "containers": [{"name": "x", "image": "y"}],
                "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                    "storageClassName": "premium-ultra-disk"
                }}}}]
            }}
        }}
    });

    let response = router
        .oneshot(create_cronjob_request("bad-storage-class", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let json = response_json(response).await;
    assert_eq!(json["kind"], "Status");
    assert_eq!(json["reason"], "Invalid");
    assert_eq!(
        json["details"]["causes"][0]["reason"],
        "FieldValueNotSupported"
    );
}

#[tokio::test]
async fn test_cronjob_with_known_storage_classes_accepted() {
    for (i, class) in ["kube-shim-standard", "kube-shim-fast"].iter().enumerate() {
        let router = test_router().await;
        let spec = json!({
            "schedule": "0 2 * * 0",
            "jobTemplate": {"spec": {
                "activeDeadlineSeconds": 3600,
                "template": {"spec": {
                    "containers": [{"name": "x", "image": "y"}],
                    "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                        "storageClassName": class
                    }}}}]
                }}
            }}
        });

        let response = router
            .oneshot(create_cronjob_request(&format!("good-{i}"), spec))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "{class} should be accepted"
        );
    }
}

#[tokio::test]
async fn test_cronjob_with_no_ephemeral_volume_still_requires_deadline() {
    // The two admission checks are independent -- a CronJob with no
    // volumes at all still needs activeDeadlineSeconds.
    let router = test_router().await;
    let spec = json!({
        "schedule": "0 2 * * 0",
        "jobTemplate": {"spec": {"template": {"spec": {"containers": [{"name": "x", "image": "y"}]}}}}
    });

    let response = router
        .oneshot(create_cronjob_request("no-volumes-no-deadline", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
