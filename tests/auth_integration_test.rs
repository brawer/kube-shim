use chrono::{Duration, Utc};
use kube_shim::config::ApiToken;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;

fn write_self_signed_cert(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("Failed to generate self-signed cert");

    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

    (cert_path, key_path)
}

/// Starts the real app router (same construction main.rs uses) over real
/// TLS on an OS-assigned port, with the given token list. Returns the base
/// HTTPS URL.
async fn start_server(api_tokens: Vec<ApiToken>) -> String {
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_self_signed_cert(&dir);
    let tls_config = kube_shim::tls::load_tls_config(&cert_path, &key_path)
        .await
        .unwrap();

    let pool = kube_shim::db::init_pool(":memory:").await.unwrap();
    let router = kube_shim::app::build_router(pool, Arc::new(api_tokens));

    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = std_listener.local_addr().unwrap();
    std_listener.set_nonblocking(true).unwrap();

    tokio::spawn(async move {
        axum_server::from_tcp_rustls(std_listener, tls_config)
            .unwrap()
            .serve(router.into_make_service())
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("https://{}", addr)
}

fn insecure_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_no_token_rejected() {
    let base_url = start_server(vec![ApiToken {
        token: "correct-token".to_string(),
        expires_at: None,
    }])
    .await;

    let resp = insecure_client()
        .get(format!("{base_url}/api/v1"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["kind"], "Status");
    assert_eq!(body["reason"], "Unauthorized");
    assert_eq!(body["code"], 401);
}

#[tokio::test]
async fn test_wrong_token_rejected() {
    let base_url = start_server(vec![ApiToken {
        token: "correct-token".to_string(),
        expires_at: None,
    }])
    .await;

    let resp = insecure_client()
        .get(format!("{base_url}/api/v1"))
        .bearer_auth("wrong-token")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_correct_token_accepted() {
    let base_url = start_server(vec![ApiToken {
        token: "correct-token".to_string(),
        expires_at: None,
    }])
    .await;

    let resp = insecure_client()
        .get(format!("{base_url}/api/v1"))
        .bearer_auth("correct-token")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["kind"], "APIResourceList");
}

#[tokio::test]
async fn test_auth_protects_secret_endpoints_too() {
    let base_url = start_server(vec![ApiToken {
        token: "correct-token".to_string(),
        expires_at: None,
    }])
    .await;

    // No auth: creating a Secret must be rejected, not just discovery reads.
    let resp = insecure_client()
        .post(format!("{base_url}/api/v1/namespaces/default/secrets"))
        .json(&serde_json::json!({
            "api_version": "v1",
            "kind": "Secret",
            "metadata": {"name": "leaked"},
            "data": {"password": "hunter2"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Listing (a read) must also require auth -- Secret contents must never
    // be visible without it.
    let resp = insecure_client()
        .get(format!("{base_url}/api/v1/namespaces/default/secrets"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn test_multiple_tokens_both_work_during_rotation() {
    let base_url = start_server(vec![
        ApiToken {
            token: "old-token".to_string(),
            expires_at: None,
        },
        ApiToken {
            token: "new-token".to_string(),
            expires_at: None,
        },
    ])
    .await;

    for tok in ["old-token", "new-token"] {
        let resp = insecure_client()
            .get(format!("{base_url}/api/v1"))
            .bearer_auth(tok)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "token {tok} should be accepted");
    }
}

#[tokio::test]
async fn test_expired_token_rejected_but_other_token_still_works() {
    let base_url = start_server(vec![
        ApiToken {
            token: "expired-token".to_string(),
            expires_at: Some(Utc::now() - Duration::days(1)),
        },
        ApiToken {
            token: "current-token".to_string(),
            expires_at: None,
        },
    ])
    .await;

    let resp = insecure_client()
        .get(format!("{base_url}/api/v1"))
        .bearer_auth("expired-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    let resp = insecure_client()
        .get(format!("{base_url}/api/v1"))
        .bearer_auth("current-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}
