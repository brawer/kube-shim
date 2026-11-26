//! Sends manifests exactly as `kubectl` and OpenTofu's Kubernetes
//! provider would (camelCase `apiVersion`, `stringData`, `type`,
//! `creationTimestamp: null`, extra metadata fields), through the real
//! router, and checks the responses use Kubernetes' field names too.

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
        Arc::new(kube_shim::reconcile::TickHealth::new()),
    )
}

fn request(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {TOKEN}"));
    match body {
        Some(body) => builder.body(Body::from(body.to_string())).unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    }
}

async fn send(router: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

const SECRETS: &str = "/api/v1/namespaces/default/secrets";

/// What `kubectl create secret generic s3 --from-literal=...` sends.
#[tokio::test]
async fn test_kubectl_create_secret_manifest_accepted() {
    let router = test_router().await;
    let manifest = json!({
        "kind": "Secret",
        "apiVersion": "v1",
        "metadata": {"name": "osmdiffs-s3-credentials", "creationTimestamp": null},
        "data": {"AWS_ACCESS_KEY_ID": "QUtJQTEyMw=="},
        "type": "Opaque"
    });
    let (status, body) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["kind"], "Secret");
    assert_eq!(body["type"], "Opaque");
    assert_eq!(body["data"]["AWS_ACCESS_KEY_ID"], "QUtJQTEyMw==");
    assert!(body.get("api_version").is_none());
    let created = body["metadata"]["creationTimestamp"].as_str().unwrap();
    assert!(
        chrono::DateTime::parse_from_rfc3339(created).is_ok(),
        "creationTimestamp {created} is not RFC 3339"
    );
    assert!(body["metadata"].get("creation_timestamp").is_none());

    let (status, body) = send(
        &router,
        request("GET", &format!("{SECRETS}/osmdiffs-s3-credentials"), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["data"]["AWS_ACCESS_KEY_ID"], "QUtJQTEyMw==");
    assert_eq!(body["metadata"]["creationTimestamp"], created);

    let (status, body) = send(&router, request("GET", SECRETS, None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["apiVersion"], "v1");
    assert_eq!(body["kind"], "SecretList");
    assert_eq!(body["items"][0]["apiVersion"], "v1");
    assert_eq!(body["items"][0]["metadata"]["creationTimestamp"], created);
}

/// `kubectl apply -f` / OpenTofu with `stringData`: plain values are
/// base64-encoded into `data`, and win over a `data` key of the same name.
#[tokio::test]
async fn test_secret_string_data_merged_into_data() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "creds",
            "namespace": "default",
            "labels": {"app": "osmdiffs"},
            "annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{}"}
        },
        "type": "Opaque",
        "data": {"keep": "a2VlcA==", "both": "b2xk"},
        "stringData": {"both": "new", "pem": "-----BEGIN KEY-----\nabc\n-----END KEY-----\n"}
    });
    let (status, body) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(body.get("stringData").is_none());

    let (_, body) = send(&router, request("GET", &format!("{SECRETS}/creds"), None)).await;
    assert_eq!(
        body["data"],
        json!({
            "keep": "a2VlcA==",
            "both": "bmV3",
            "pem": "LS0tLS1CRUdJTiBLRVktLS0tLQphYmMKLS0tLS1FTkQgS0VZLS0tLS0K"
        })
    );
}

#[tokio::test]
async fn test_secret_with_only_string_data_accepted() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "plain"},
        "stringData": {"password": "secret123"}
    });
    let (status, body) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["data"], json!({"password": "c2VjcmV0MTIz"}));
}

#[tokio::test]
async fn test_secret_with_plain_data_value_rejected() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "bad"},
        "data": {"password": "not base64!"}
    });
    let (status, _) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_secret_with_non_string_string_data_rejected() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "bad"},
        "stringData": {"port": 5432}
    });
    let (status, _) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// No compatibility alias: the old snake_case field is gone.
#[tokio::test]
async fn test_snake_case_api_version_rejected() {
    let router = test_router().await;
    let manifest = json!({
        "api_version": "v1",
        "kind": "Secret",
        "metadata": {"name": "old"},
        "stringData": {"a": "b"}
    });
    let (status, _) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert!(status.is_client_error(), "got {status}");
}

/// The osmdiffs CronJob shape, as `kubectl apply` or OpenTofu's
/// `kubernetes_cron_job_v1` sends it.
#[tokio::test]
async fn test_kubectl_cronjob_manifest_accepted() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "osmdiffs",
            "namespace": "default",
            "creationTimestamp": null,
            "labels": {"app": "osmdiffs"}
        },
        "spec": {
            "schedule": "*/5 * * * *",
            "concurrencyPolicy": "Forbid",
            "jobTemplate": {"spec": {
                "activeDeadlineSeconds": 3600,
                "template": {"spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "osmdiffs",
                        "image": "busybox:latest",
                        "envFrom": [{"secretRef": {"name": "osmdiffs-s3-credentials"}}]
                    }]
                }}
            }}
        }
    });
    let uri = "/apis/batch/v1/namespaces/default/cronjobs";
    let (status, body) = send(&router, request("POST", uri, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["apiVersion"], "batch/v1");
    assert!(body.get("api_version").is_none());
    let created = body["metadata"]["creationTimestamp"].as_str().unwrap();
    assert!(chrono::DateTime::parse_from_rfc3339(created).is_ok());

    let created = created.to_string();

    let (status, body) = send(&router, request("GET", uri, None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["apiVersion"], "batch/v1");
    assert_eq!(body["items"][0]["apiVersion"], "batch/v1");
    assert_eq!(body["items"][0]["metadata"]["creationTimestamp"], created);
}

#[tokio::test]
async fn test_kubectl_job_manifest_accepted() {
    let router = test_router().await;
    let manifest = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "manual-run", "creationTimestamp": null},
        "spec": {
            "activeDeadlineSeconds": 600,
            "template": {"spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "main", "image": "busybox:latest"}]
            }}
        }
    });
    let uri = "/apis/batch/v1/namespaces/default/jobs";
    let (status, body) = send(&router, request("POST", uri, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["apiVersion"], "batch/v1");
    assert!(body.get("api_version").is_none());
    let created = body["metadata"]["creationTimestamp"].as_str().unwrap();
    assert!(chrono::DateTime::parse_from_rfc3339(created).is_ok());

    // Issue #154: `kubectl get jobs` showed AGE <unknown>, and
    // `--sort-by=.metadata.creationTimestamp` failed, on the list.
    let (status, list) = send(&router, request("GET", uri, None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["items"][0]["metadata"]["creationTimestamp"], created);
}

/// What `kubectl get` sends: it prints the server's columns from a
/// `Table`, and only NAME and AGE without one.
const KUBECTL_GET_ACCEPT: &str = "application/json;as=Table;v=v1;g=meta.k8s.io,\
     application/json;as=Table;v=v1beta1;g=meta.k8s.io,application/json";

fn table_request(uri: &str) -> Request<Body> {
    let mut req = request("GET", uri, None);
    req.headers_mut()
        .insert("accept", KUBECTL_GET_ACCEPT.parse().unwrap());
    req
}

fn column_names(table: &Value) -> Vec<&str> {
    table["columnDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn test_kubectl_get_receives_tables() {
    let router = test_router().await;
    let jobs = "/apis/batch/v1/namespaces/default/jobs";
    let manifest = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "manual-run", "labels": {"app": "osm"}},
        "spec": {
            "activeDeadlineSeconds": 600,
            "template": {"spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "main", "image": "busybox:latest"}]
            }}
        }
    });
    let (status, job) = send(&router, request("POST", jobs, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    let uid = job["metadata"]["uid"].as_str().unwrap();

    let (status, table) = send(&router, table_request(jobs)).await;
    assert_eq!(status, StatusCode::OK, "{table}");
    assert_eq!(table["kind"], "Table");
    assert_eq!(table["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(
        column_names(&table),
        [
            "Name",
            "Status",
            "Completions",
            "Duration",
            "Age",
            "Containers",
            "Images",
            "Selector"
        ]
    );
    let cells = &table["rows"][0]["cells"];
    assert_eq!(cells[0], "manual-run");
    assert_eq!(cells[1], "Running");
    assert_eq!(cells[2], "0/1");
    assert_eq!(cells[5], "main");
    assert_eq!(cells[6], "busybox:latest");
    assert_eq!(
        cells[7],
        format!("batch.kubernetes.io/controller-uid={uid}")
    );
    let object = &table["rows"][0]["object"];
    assert_eq!(object["kind"], "PartialObjectMetadata");
    assert_eq!(object["metadata"]["labels"]["app"], "osm");

    // A single object is a one-row Table too.
    let (status, table) = send(&router, table_request(&format!("{jobs}/manual-run"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(table["kind"], "Table");
    assert_eq!(table["rows"].as_array().unwrap().len(), 1);

    // Without `as=Table`, the plain object as before.
    let (_, list) = send(&router, request("GET", jobs, None)).await;
    assert_eq!(list["kind"], "JobList");

    // Errors stay a Status.
    let (status, body) = send(&router, table_request(&format!("{jobs}/nope"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_ne!(body["kind"], "Table");

    let (status, body) = send(
        &router,
        table_request(&format!("{jobs}?includeObject=Everything")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["kind"], "Status");

    let manifest = json!({
        "kind": "Secret",
        "apiVersion": "v1",
        "metadata": {"name": "creds"},
        "data": {"a": "eA==", "b": "eQ=="},
        "type": "Opaque"
    });
    let (status, _) = send(&router, request("POST", SECRETS, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, table) = send(&router, table_request(SECRETS)).await;
    assert_eq!(column_names(&table), ["Name", "Type", "Data", "Age"]);
    assert_eq!(table["rows"][0]["cells"][1], "Opaque");
    assert_eq!(table["rows"][0]["cells"][2], 2);

    let cronjobs = "/apis/batch/v1/namespaces/default/cronjobs";
    let manifest = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "nightly"},
        "spec": {
            "schedule": "0 3 * * *",
            "jobTemplate": {"spec": {
                "activeDeadlineSeconds": 600,
                "template": {"spec": {
                    "restartPolicy": "Never",
                    "containers": [{"name": "main", "image": "busybox:latest"}]
                }}
            }}
        }
    });
    let (status, body) = send(&router, request("POST", cronjobs, Some(manifest))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (_, table) = send(&router, table_request(cronjobs)).await;
    assert_eq!(
        column_names(&table),
        [
            "Name",
            "Schedule",
            "Timezone",
            "Suspend",
            "Active",
            "Last Schedule",
            "Age",
            "Containers",
            "Images",
            "Selector"
        ]
    );
    let cells = &table["rows"][0]["cells"];
    assert_eq!(cells[0], "nightly");
    assert_eq!(cells[1], "0 3 * * *");
    assert_eq!(cells[5], "<none>");
    assert_eq!(cells[7], "main");
}
