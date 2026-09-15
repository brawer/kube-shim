use axum::{routing::get, Json, Router};
use serde_json::json;
use std::net::{SocketAddr, TcpListener as StdTcpListener};

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "healthy"}))
}

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

#[tokio::test]
async fn test_server_serves_https_not_plain_http() {
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_self_signed_cert(&dir);

    let tls_config = kube_shim::tls::load_tls_config(&cert_path, &key_path)
        .await
        .expect("Failed to load TLS config");

    // Bind a std listener on an OS-assigned free port so we know the real
    // address before the server starts.
    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = std_listener.local_addr().unwrap();
    std_listener.set_nonblocking(true).unwrap();

    let app = Router::new().route("/health", get(health));

    tokio::spawn(async move {
        axum_server::from_tcp_rustls(std_listener, tls_config)
            .expect("Failed to construct rustls server from listener")
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // A self-signed cert isn't in any trust store, so the test client must
    // explicitly accept it -- that's the point of the test: confirm the
    // server negotiates a real TLS handshake, not that the cert is trusted
    // by a public CA.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let url = format!("https://{}/health", addr);
    let resp = client.get(&url).send().await.expect("HTTPS request failed");
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "healthy");

    // The same port must not also speak plain HTTP -- a client that skips
    // the TLS handshake entirely should fail to get a valid HTTP response
    // rather than being silently served in the clear.
    let plain_url = format!("http://{}/health", addr);
    let plain_result = client.get(&plain_url).send().await;
    assert!(
        plain_result.is_err(),
        "server must not respond to plain HTTP on the TLS port"
    );
}
