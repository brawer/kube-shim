//! Kubernetes protobuf request bodies (issue #106), what the
//! `hashicorp/kubernetes` provider sends for every built-in type.
//!
//! The fixtures in tests/testdata/protobuf come from
//! tools/k8s-proto-gen: each `.pb` is a body encoded the way client-go
//! encodes it, and the matching `.json` is what Go itself writes after
//! decoding those bytes. Converting the `.pb` must give exactly the
//! `.json`, and the router must treat both the same.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use kube_shim::config::ApiToken;
use kube_shim::k8s_protobuf;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const TOKEN: &str = "test-token";
const PROTOBUF: &str = "application/vnd.kubernetes.protobuf";
const SECRETS: &str = "/api/v1/namespaces/default/secrets";
const CRONJOBS: &str = "/apis/batch/v1/namespaces/default/cronjobs";
const JOBS: &str = "/apis/batch/v1/namespaces/default/jobs";

const FIXTURES: &[(&str, &str)] = &[
    ("secret", SECRETS),
    ("cronjob", CRONJOBS),
    ("job", JOBS),
    ("job-no-deadline", JOBS),
    ("job-misc", JOBS),
];

fn fixture_pb(name: &str) -> Vec<u8> {
    std::fs::read(format!("tests/testdata/protobuf/{name}.pb")).unwrap()
}

fn fixture_json(name: &str) -> Value {
    let text = std::fs::read_to_string(format!("tests/testdata/protobuf/{name}.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}

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

fn request(method: &str, uri: &str, content_type: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", content_type)
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(body.into())
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

/// Removes what the server generates itself, so a protobuf-created
/// object can be compared with a JSON-created one.
fn strip_generated(mut object: Value) -> Value {
    if let Some(meta) = object.get_mut("metadata").and_then(Value::as_object_mut) {
        meta.remove("uid");
        meta.remove("creationTimestamp");
    }
    object
}

// --- Minimal protobuf encoding, to build envelopes the fixtures don't cover.

fn varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn len_field(number: u64, bytes: &[u8], out: &mut Vec<u8>) {
    varint(number << 3 | 2, out);
    varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

fn varint_field(number: u64, value: u64, out: &mut Vec<u8>) {
    varint(number << 3, out);
    varint(value, out);
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> u64 {
    let mut n = 0;
    let mut shift = 0;
    loop {
        let b = bytes[*pos];
        *pos += 1;
        n |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return n;
        }
        shift += 7;
    }
}

/// The `raw` object bytes of a fixture's `runtime.Unknown` envelope.
fn raw_of(pb: &[u8]) -> Vec<u8> {
    let body = &pb[4..];
    let mut pos = 0;
    while pos < body.len() {
        let key = read_varint(body, &mut pos);
        assert_eq!(
            key & 7,
            2,
            "runtime.Unknown only has length-delimited fields"
        );
        let len = read_varint(body, &mut pos) as usize;
        if key >> 3 == 2 {
            return body[pos..pos + len].to_vec();
        }
        pos += len;
    }
    panic!("no raw field");
}

fn envelope(api_version: &str, kind: &str, raw: &[u8]) -> Vec<u8> {
    let mut type_meta = Vec::new();
    len_field(1, api_version.as_bytes(), &mut type_meta);
    len_field(2, kind.as_bytes(), &mut type_meta);
    let mut out = b"k8s\0".to_vec();
    len_field(1, &type_meta, &mut out);
    len_field(2, raw, &mut out);
    out
}

#[test]
fn test_fixtures_decode_to_go_json() {
    for (name, _) in FIXTURES {
        let (_, json) = k8s_protobuf::decode(&fixture_pb(name)).unwrap();
        assert_eq!(json, fixture_json(name), "{name}");
    }
}

#[test]
fn test_int64_stays_a_number() {
    let (_, json) = k8s_protobuf::decode(&fixture_pb("cronjob")).unwrap();
    let job_spec = json.pointer("/spec/jobTemplate/spec").unwrap();
    assert_eq!(job_spec["activeDeadlineSeconds"].as_i64(), Some(57600));
    assert_eq!(
        job_spec.pointer("/template/spec/securityContext/fsGroup"),
        Some(&Value::from(1000))
    );
}

#[test]
fn test_go_zero_values_are_dropped_and_pointers_kept() {
    let (_, json) = k8s_protobuf::decode(&fixture_pb("cronjob")).unwrap();
    let container = json
        .pointer("/spec/jobTemplate/spec/template/spec/containers/0")
        .unwrap();
    // Non-pointer omitempty fields the client never set: absent.
    assert_eq!(container.get("workingDir"), None);
    assert_eq!(container.get("stdin"), None);
    assert_eq!(container.get("imagePullPolicy"), None);
    // Pointer fields set to their zero value: present.
    assert_eq!(
        container.pointer("/securityContext/privileged"),
        Some(&Value::Bool(false))
    );
    assert_eq!(json.pointer("/spec/suspend"), Some(&Value::Bool(false)));
}

#[test]
fn test_unknown_top_level_field_rejected() {
    let mut raw = raw_of(&fixture_pb("secret"));
    varint_field(999, 1, &mut raw);
    let err = k8s_protobuf::decode(&envelope("v1", "Secret", &raw)).unwrap_err();
    assert!(err.contains("999"), "{err}");
}

#[test]
fn test_unknown_nested_field_rejected() {
    // A second occurrence of field 1 (metadata) is merged into the
    // first, so this puts an unknown field 99 inside ObjectMeta.
    let mut raw = raw_of(&fixture_pb("secret"));
    let mut meta = Vec::new();
    varint_field(99, 1, &mut meta);
    len_field(1, &meta, &mut raw);
    let err = k8s_protobuf::decode(&envelope("v1", "Secret", &raw)).unwrap_err();
    assert!(err.contains("metadata") && err.contains("99"), "{err}");
}

#[test]
fn test_unsupported_kind_rejected() {
    let raw = raw_of(&fixture_pb("secret"));
    let err = k8s_protobuf::decode(&envelope("v1", "ConfigMap", &raw)).unwrap_err();
    assert!(err.contains("v1/ConfigMap"), "{err}");
}

/// Each fixture gets the same answer from the router as protobuf as it
/// does as JSON: same status, same object back.
#[tokio::test]
async fn test_router_treats_protobuf_like_json() {
    for (name, uri) in FIXTURES {
        let via_json = send(
            &test_router().await,
            request(
                "POST",
                uri,
                "application/json",
                fixture_json(name).to_string(),
            ),
        )
        .await;
        let via_protobuf = send(
            &test_router().await,
            request("POST", uri, PROTOBUF, fixture_pb(name)),
        )
        .await;
        let expected = if *name == "job-no-deadline" {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::CREATED
        };
        assert_eq!(via_json.0, expected, "{name}: {:?}", via_json.1);
        assert_eq!(via_protobuf.0, via_json.0, "{name}: {:?}", via_protobuf.1);
        assert_eq!(
            strip_generated(via_protobuf.1),
            strip_generated(via_json.1),
            "{name}"
        );
    }
}

/// What production's kube-shim-jobs/ needs: the Secret and the CronJob
/// are created, and the stored objects are what the client sent.
#[tokio::test]
async fn test_osmdiffs_objects_created_from_protobuf() {
    let router = test_router().await;
    let (status, secret) = send(
        &router,
        request("POST", SECRETS, PROTOBUF, fixture_pb("secret")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{secret}");
    assert_eq!(secret["data"], fixture_json("secret")["data"]);

    let (status, created) = send(
        &router,
        request("POST", CRONJOBS, PROTOBUF, fixture_pb("cronjob")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let get = Request::builder()
        .uri(format!("{CRONJOBS}/osmdiffs-weekly"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let (status, fetched) = send(&router, get).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["spec"], fixture_json("cronjob")["spec"]);

    // The provider updates a CronJob with a full-object PUT, also protobuf.
    let (status, replaced) = send(
        &router,
        request(
            "PUT",
            &format!("{CRONJOBS}/osmdiffs-weekly"),
            PROTOBUF,
            fixture_pb("cronjob"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replaced}");
}

#[tokio::test]
async fn test_protobuf_body_for_wrong_endpoint_rejected() {
    let (status, body) = send(
        &test_router().await,
        request("POST", CRONJOBS, PROTOBUF, fixture_pb("secret")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["kind"], "Status");
    assert!(
        body["message"].as_str().unwrap().contains("v1/Secret"),
        "{body}"
    );
}

#[tokio::test]
async fn test_protobuf_on_unsupported_endpoint_is_415() {
    let (status, body) = send(
        &test_router().await,
        request(
            "POST",
            "/apis/cost.kube-shim.brawer.ch/v1/budget/topup",
            PROTOBUF,
            fixture_pb("secret"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(body["reason"], "UnsupportedMediaType");
}

#[tokio::test]
async fn test_garbage_protobuf_body_rejected() {
    let (status, body) = send(
        &test_router().await,
        request("POST", SECRETS, PROTOBUF, b"k8s\0\xff\xff\xff".to_vec()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["kind"], "Status");
}

#[tokio::test]
async fn test_other_media_types_get_415_status() {
    let (status, body) = send(
        &test_router().await,
        request("POST", SECRETS, "text/plain", "hello"),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["reason"], "UnsupportedMediaType");
}

/// client-go sends `DeleteOptions` as a protobuf body; the handlers
/// ignore it, whatever it holds.
#[tokio::test]
async fn test_delete_ignores_protobuf_body() {
    let router = test_router().await;
    let (status, _) = send(
        &router,
        request("POST", SECRETS, PROTOBUF, fixture_pb("secret")),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        &router,
        request(
            "DELETE",
            &format!("{SECRETS}/osmdiffs-s3-credentials"),
            PROTOBUF,
            b"k8s\0not a DeleteOptions".to_vec(),
        ),
    )
    .await;
    assert!(status.is_success(), "{status} {body}");
}
