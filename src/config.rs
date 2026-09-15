use anyhow::{Context, Result};
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
    pub tls_cert_path: String,
    pub tls_key_path: String,
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
        let config_str = std::fs::read_to_string(path)
            .context("Failed to read config file")?;

        let config: Config = toml::from_str(&config_str)
            .context("Failed to parse TOML config")?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parse() {
        let toml_str = r#"
[server]
host = "127.0.0.1"
port = 6443
tls_cert_path = "/path/to/cert.pem"
tls_key_path = "/path/to/key.pem"

[database]
path = "db.sqlite"

[hetzner]
token = "test-token"
dry_run = true

[reconciliation]
interval_secs = 10
"#;
        let config: Config = toml::from_str(toml_str).expect("Failed to parse config");
        assert_eq!(config.server.port, 6443);
        assert_eq!(config.hetzner.dry_run, true);
    }
}
