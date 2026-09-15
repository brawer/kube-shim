use anyhow::{bail, Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use std::path::Path;

/// Loads a TLS server configuration from a PEM-encoded certificate chain and
/// private key file. Validates that both files exist and contain well-formed
/// PEM before handing off to `RustlsConfig`, so a misconfigured deployment
/// fails fast at startup with a clear message rather than an opaque error
/// from deep inside the TLS stack.
pub async fn load_tls_config<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<RustlsConfig> {
    // rustls needs exactly one process-wide crypto provider installed before
    // building any TLS config. More than one provider can end up compiled in
    // transitively (e.g. via a dependency that specifically needs `ring`),
    // which leaves rustls unable to pick one automatically. `ring` is chosen
    // explicitly here -- unlike `aws-lc-rs`, it doesn't need a C/cmake
    // toolchain at build time, which matters for cross-compiling to musl for
    // the scratch container image (see docs/IMPLEMENTATION_PLAN.md Phase 3).
    // Installing twice (e.g. across multiple tests in one process) is fine;
    // the second call just returns an error that we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();

    let cert_bytes = std::fs::read(cert_path)
        .with_context(|| format!("Failed to read TLS certificate at {}", cert_path.display()))?;
    let key_bytes = std::fs::read(key_path)
        .with_context(|| format!("Failed to read TLS private key at {}", key_path.display()))?;

    let cert_count = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("{} does not contain valid PEM certificate data", cert_path.display()))?
        .len();
    if cert_count == 0 {
        bail!("{} contains no certificates", cert_path.display());
    }

    let key_count = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .with_context(|| format!("{} does not contain valid PEM private key data", key_path.display()))?;
    if key_count.is_none() {
        bail!("{} contains no private key", key_path.display());
    }

    RustlsConfig::from_pem(cert_bytes, key_bytes)
        .await
        .with_context(|| {
            format!(
                "Failed to build TLS server config from {} and {}",
                cert_path.display(),
                key_path.display()
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_self_signed_cert(dir: &tempfile::TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                .expect("Failed to generate self-signed cert");

        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        std::fs::File::create(&cert_path)
            .unwrap()
            .write_all(cert.pem().as_bytes())
            .unwrap();
        std::fs::File::create(&key_path)
            .unwrap()
            .write_all(key_pair.serialize_pem().as_bytes())
            .unwrap();

        (cert_path, key_path)
    }

    #[tokio::test]
    async fn test_load_valid_cert_and_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed_cert(&dir);

        let result = load_tls_config(&cert_path, &key_path).await;
        assert!(result.is_ok(), "expected valid cert/key to load: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_missing_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let (_cert_path, key_path) = write_self_signed_cert(&dir);
        let missing = dir.path().join("does-not-exist.pem");

        let result = load_tls_config(&missing, &key_path).await;
        let err = result.expect_err("expected missing cert file to error");
        assert!(err.to_string().contains("does-not-exist.pem"));
    }

    #[tokio::test]
    async fn test_missing_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, _key_path) = write_self_signed_cert(&dir);
        let missing = dir.path().join("does-not-exist.pem");

        let result = load_tls_config(&cert_path, &missing).await;
        let err = result.expect_err("expected missing key file to error");
        assert!(err.to_string().contains("does-not-exist.pem"));
    }

    #[tokio::test]
    async fn test_invalid_pem_cert() {
        let dir = tempfile::tempdir().unwrap();
        let (_cert_path, key_path) = write_self_signed_cert(&dir);

        let garbage_cert_path = dir.path().join("garbage-cert.pem");
        std::fs::write(&garbage_cert_path, b"this is not a certificate").unwrap();

        let result = load_tls_config(&garbage_cert_path, &key_path).await;
        assert!(result.is_err(), "expected garbage cert data to error, not panic");
    }

    #[tokio::test]
    async fn test_invalid_pem_key() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, _key_path) = write_self_signed_cert(&dir);

        let garbage_key_path = dir.path().join("garbage-key.pem");
        std::fs::write(&garbage_key_path, b"this is not a private key").unwrap();

        let result = load_tls_config(&cert_path, &garbage_key_path).await;
        assert!(result.is_err(), "expected garbage key data to error, not panic");
    }

    #[tokio::test]
    async fn test_empty_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let (_cert_path, key_path) = write_self_signed_cert(&dir);

        let empty_cert_path = dir.path().join("empty-cert.pem");
        std::fs::write(&empty_cert_path, b"").unwrap();

        let result = load_tls_config(&empty_cert_path, &key_path).await;
        let err = result.expect_err("expected empty cert file to error");
        assert!(err.to_string().contains("no certificates"));
    }
}
