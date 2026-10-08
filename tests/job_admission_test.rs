//! Exercises Phase 13's standalone-Job admission checks through the
//! real router (same construction main.rs uses), not just the
//! underlying functions directly -- confirms the generalized
//! `admission` checks are actually wired into the HTTP path for a Job's
//! own (unwrapped) `.spec`, with the right field-path prefix, not just
//! correct in isolation. Mirrors `cronjob_admission_test.rs`'s own
//! structure, one resource over.

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
    )
}

fn create_job_request(name: &str, spec: Value) -> Request<Body> {
    let body = json!({
        "api_version": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": "default"},
        "spec": spec,
    });
    Request::builder()
        .method("POST")
        .uri("/apis/batch/v1/namespaces/default/jobs")
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
async fn test_job_without_active_deadline_seconds_rejected() {
    let router = test_router().await;
    let spec = json!({"template": {"spec": {
        "containers": [{"name": "x", "image": "y"}]
    }}});

    let response = router
        .oneshot(create_job_request("no-deadline", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let json = response_json(response).await;
    assert_eq!(json["kind"], "Status");
    assert_eq!(json["reason"], "Forbidden");
    let message = json["message"].as_str().unwrap();
    assert!(message.contains("activeDeadlineSeconds"));
    // The field path must be the Job's own "spec...", not the CronJob
    // caller's "spec.jobTemplate.spec..." -- this is the one real thing
    // the admission.rs generalization needs to get right.
    assert!(message.contains("spec.activeDeadlineSeconds"));
    assert!(!message.contains("jobTemplate"));
}

#[tokio::test]
async fn test_job_with_active_deadline_seconds_accepted() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{"name": "x", "image": "y"}]}}
    });

    let response = router
        .oneshot(create_job_request("with-deadline", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_job_with_unknown_storage_class_rejected() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {
            "containers": [{"name": "x", "image": "y"}],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "storageClassName": "premium-ultra-disk"
            }}}}]
        }}
    });

    let response = router
        .oneshot(create_job_request("bad-storage-class", spec))
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
    assert!(!json["message"].as_str().unwrap().contains("jobTemplate"));
}

#[tokio::test]
async fn test_job_with_known_storage_classes_accepted() {
    for (i, class) in ["kube-shim-standard", "kube-shim-fast"].iter().enumerate() {
        let router = test_router().await;
        let spec = json!({
            "activeDeadlineSeconds": 3600,
            "template": {"spec": {
                "containers": [{"name": "x", "image": "y"}],
                "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                    "storageClassName": class
                }}}}]
            }}
        });

        let response = router
            .oneshot(create_job_request(&format!("good-{i}"), spec))
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
async fn test_job_with_malformed_resource_limit_rejected() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{
            "name": "x", "image": "y",
            "resources": {"limits": {"cpu": "not-a-quantity"}}
        }]}}
    });

    let response = router
        .oneshot(create_job_request("bad-cpu-limit", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let json = response_json(response).await;
    assert_eq!(json["reason"], "Invalid");
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("resources.limits.cpu"));
}

#[tokio::test]
async fn test_job_with_valid_resource_limits_accepted() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{
            "name": "x", "image": "y",
            "resources": {"limits": {"cpu": "500m", "memory": "512Mi"}}
        }]}}
    });

    let response = router
        .oneshot(create_job_request("good-limits", spec))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_duplicate_job_name_in_same_namespace_rejected() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{"name": "x", "image": "y"}]}}
    });

    let first = router
        .clone()
        .oneshot(create_job_request("dup", spec.clone()))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);

    let second = router
        .oneshot(create_job_request("dup", spec))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = response_json(second).await;
    assert_eq!(json["reason"], "AlreadyExists");
}

#[tokio::test]
async fn test_job_full_lifecycle_create_get_list_delete() {
    let router = test_router().await;
    let spec = json!({
        "activeDeadlineSeconds": 3600,
        "template": {"spec": {"containers": [{"name": "x", "image": "y"}]}}
    });

    let create = router
        .clone()
        .oneshot(create_job_request("lifecycle-test", spec))
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let get = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/apis/batch/v1/namespaces/default/jobs/lifecycle-test")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let job = response_json(get).await;
    assert_eq!(job["metadata"]["name"], "lifecycle-test");
    assert_eq!(job["status"]["active"], 1);

    let list = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/apis/batch/v1/namespaces/default/jobs")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let list_body = response_json(list).await;
    assert_eq!(list_body["items"].as_array().unwrap().len(), 1);

    let delete = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/apis/batch/v1/namespaces/default/jobs/lifecycle-test")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}
