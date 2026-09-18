use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub hetzner: HetznerConfig,
    pub reconciliation: ReconciliationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Self-signed certificate used only as a fallback when `hostname`
    /// below is unset -- e.g. local dev/CI, where there's no public DNS
    /// for ACME to issue a real certificate against.
    pub tls_cert_path: String,
    pub tls_key_path: String,
    /// Bearer tokens accepted on the authenticated API. A *list*, not a
    /// single token, so a token can be rotated with an overlap window
    /// (add the new one, migrate clients, then remove the old one) instead
    /// of one atomic cutover -- mirrors Kubernetes' own built-in
    /// static-token-file authenticator.
    pub api_tokens: Vec<ApiToken>,
    /// Real public hostname to request an ACME (Let's Encrypt) certificate
    /// for, Caddy-like. When unset, ACME is skipped entirely and the shim
    /// falls back to the self-signed certificate above -- this is what
    /// keeps local dev/CI working with no public DNS at all. All fields
    /// below this one are only consulted when `hostname` is set.
    #[serde(default)]
    pub hostname: Option<String>,
    /// "staging" or "production" select Let's Encrypt's own two
    /// environments (staging has much higher rate limits but issues a
    /// certificate chain that isn't publicly trusted -- safe to hammer
    /// while testing); any other value is used verbatim as a custom ACME
    /// directory URL (e.g. a local Pebble test server).
    #[serde(default = "default_acme_directory")]
    pub acme_directory: String,
    /// Optional contact email for the ACME account. Let's Encrypt only
    /// ever uses it for expiry/problem notifications; never required.
    #[serde(default)]
    pub acme_contact_email: Option<String>,
    /// Port for the minimal HTTP-01 challenge responder -- a separate
    /// listener/router from the authenticated API, not a path carved out
    /// of it (see docs/IMPLEMENTATION_PLAN.md Phase 4).
    #[serde(default = "default_acme_challenge_port")]
    pub acme_challenge_port: u16,
    /// Where the ACME account key and issued certificates are cached
    /// across restarts. Without this, every restart would re-issue a
    /// certificate from scratch and risk Let's Encrypt's rate limits.
    #[serde(default = "default_acme_cache_dir")]
    pub acme_cache_dir: String,
}

fn default_acme_directory() -> String {
    "staging".to_string()
}

fn default_acme_challenge_port() -> u16 {
    8080
}

fn default_acme_cache_dir() -> String {
    "acme-cache".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    pub token: String,
    /// RFC 3339 timestamp. Omitted means the token never expires.
    pub expires_at: Option<DateTime<Utc>>,
}

impl ApiToken {
    pub fn is_valid_at(&self, now: DateTime<Utc>) -> bool {
        match self.expires_at {
            Some(expires_at) => now < expires_at,
            None => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HetznerConfig {
    pub token: String,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationConfig {
    pub interval_secs: u64,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config_str = std::fs::read_to_string(path).context("Failed to read config file")?;

        let config: Config = toml::from_str(&config_str).context("Failed to parse TOML config")?;

        config.ensure_has_valid_api_token(Utc::now())?;

        Ok(config)
    }

    /// Refuses a config with no currently-valid bearer token, rather than
    /// letting the shim boot with either no effective auth (failing open)
    /// or a confusing "every request gets 401" state discovered only once
    /// something tries to connect.
    fn ensure_has_valid_api_token(&self, now: DateTime<Utc>) -> Result<()> {
        if !self.server.api_tokens.iter().any(|t| t.is_valid_at(now)) {
            bail!(
                "server.api_tokens has no currently-valid token (list is empty, or every \
                 entry has expired) -- refusing to start with no effective authentication"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn base_toml() -> String {
        r#"
[server]
host = "127.0.0.1"
port = 6443
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"

[[server.api_tokens]]
token = "test-token"

[database]
path = "db.sqlite"

[hetzner]
token = "test-token"
dry_run = true

[reconciliation]
interval_secs = 10
"#
        .to_string()
    }

    #[test]
    fn test_config_parse() {
        let config: Config = toml::from_str(&base_toml()).expect("Failed to parse config");
        assert_eq!(config.server.port, 6443);
        assert!(config.hetzner.dry_run);
        assert_eq!(config.server.api_tokens.len(), 1);
        assert_eq!(config.server.api_tokens[0].token, "test-token");
        assert!(config.server.api_tokens[0].expires_at.is_none());
    }

    #[test]
    fn test_config_parse_multiple_tokens_with_expiry() {
        let toml_str = r#"
[server]
host = "127.0.0.1"
port = 6443
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"

[[server.api_tokens]]
token = "old-token"
expires_at = "2020-01-01T00:00:00Z"

[[server.api_tokens]]
token = "new-token"

[database]
path = "db.sqlite"

[hetzner]
token = "test-token"
dry_run = true

[reconciliation]
interval_secs = 10
"#;
        let config: Config = toml::from_str(toml_str).expect("Failed to parse config");
        assert_eq!(config.server.api_tokens.len(), 2);

        let now = Utc::now();
        assert!(!config.server.api_tokens[0].is_valid_at(now)); // expired
        assert!(config.server.api_tokens[1].is_valid_at(now)); // no expiry
    }

    #[test]
    fn test_api_token_expiry() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

        let never_expires = ApiToken {
            token: "a".to_string(),
            expires_at: None,
        };
        assert!(never_expires.is_valid_at(now));

        let future = ApiToken {
            token: "b".to_string(),
            expires_at: Some(now + Duration::days(1)),
        };
        assert!(future.is_valid_at(now));

        let past = ApiToken {
            token: "c".to_string(),
            expires_at: Some(now - Duration::days(1)),
        };
        assert!(!past.is_valid_at(now));
    }

    #[test]
    fn test_ensure_has_valid_api_token_empty_list_rejected() {
        let mut config: Config = toml::from_str(&base_toml()).unwrap();
        config.server.api_tokens.clear();
        let err = config
            .ensure_has_valid_api_token(Utc::now())
            .expect_err("empty token list must be rejected");
        assert!(err.to_string().contains("no currently-valid token"));
    }

    #[test]
    fn test_ensure_has_valid_api_token_all_expired_rejected() {
        let mut config: Config = toml::from_str(&base_toml()).unwrap();
        let now = Utc::now();
        config.server.api_tokens = vec![ApiToken {
            token: "expired".to_string(),
            expires_at: Some(now - Duration::days(1)),
        }];
        assert!(config.ensure_has_valid_api_token(now).is_err());
    }

    #[test]
    fn test_ensure_has_valid_api_token_accepts_one_valid_among_expired() {
        let mut config: Config = toml::from_str(&base_toml()).unwrap();
        let now = Utc::now();
        config.server.api_tokens = vec![
            ApiToken {
                token: "expired".to_string(),
                expires_at: Some(now - Duration::days(1)),
            },
            ApiToken {
                token: "valid".to_string(),
                expires_at: None,
            },
        ];
        assert!(config.ensure_has_valid_api_token(now).is_ok());
    }

    #[test]
    fn test_acme_fields_default_when_absent() {
        // The already-deployed kube-shim.brawer.ch config.toml predates
        // these fields entirely -- this must keep parsing, and must default
        // to "ACME disabled, self-signed fallback" (hostname: None), not
        // fail closed or silently enable ACME with no hostname to target.
        let config: Config = toml::from_str(&base_toml()).expect("Failed to parse config");
        assert!(config.server.hostname.is_none());
        assert_eq!(config.server.acme_directory, "staging");
        assert!(config.server.acme_contact_email.is_none());
        assert_eq!(config.server.acme_challenge_port, 8080);
        assert_eq!(config.server.acme_cache_dir, "acme-cache");
    }

    #[test]
    fn test_acme_fields_explicit() {
        let toml_str = r#"
[server]
host = "0.0.0.0"
port = 8443
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"
hostname = "kube-shim.brawer.ch"
acme_directory = "production"
acme_contact_email = "sascha@example.com"
acme_challenge_port = 8080
acme_cache_dir = "/data/acme-cache"

[[server.api_tokens]]
token = "test-token"

[database]
path = "db.sqlite"

[hetzner]
token = "test-token"
dry_run = true

[reconciliation]
interval_secs = 10
"#;
        let config: Config = toml::from_str(toml_str).expect("Failed to parse config");
        assert_eq!(
            config.server.hostname.as_deref(),
            Some("kube-shim.brawer.ch")
        );
        assert_eq!(config.server.acme_directory, "production");
        assert_eq!(
            config.server.acme_contact_email.as_deref(),
            Some("sascha@example.com")
        );
        assert_eq!(config.server.acme_challenge_port, 8080);
        assert_eq!(config.server.acme_cache_dir, "/data/acme-cache");
    }

    #[test]
    fn test_from_file_rejects_config_with_no_valid_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let toml_str = r#"
[server]
host = "127.0.0.1"
port = 6443
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"
api_tokens = []

[database]
path = "db.sqlite"

[hetzner]
token = "test-token"
dry_run = true

[reconciliation]
interval_secs = 10
"#;
        std::fs::write(&path, toml_str).unwrap();

        let err = Config::from_file(&path).expect_err("empty api_tokens must fail to load");
        assert!(err.to_string().contains("no currently-valid token"));
    }
}
