//! The shim's own public IPv4, as reported by UpCloud's per-server
//! metadata service (Phase 9). Used to build the worker-VM firewall's
//! inbound-SSH-from-the-shim rule (`reconcile::job`'s `FirewallApplying`
//! handler) without ever hardcoding an IP in config -- deliberately, after
//! this project's own shim instance changed public IPv4 mid-project (see
//! the server-migration work in docs/IMPLEMENTATION_PLAN.md's history):
//! a static config value would have gone silently stale at exactly that
//! moment, quietly leaving every subsequent worker VM's SSH rule pointing
//! at an address that was no longer the shim's own.
//!
//! Every UpCloud server with `metadata: "yes"` (this shim's own instance
//! has had that set since Phase 7/9's server creation code) can query
//! `http://169.254.169.254/metadata/v1.json` from within itself to learn
//! its own current configuration, including its assigned IP addresses --
//! see <https://upcloud.com/docs/guides/upcloud-metadata-service/>. This
//! is UpCloud-specific (the endpoint, not just the data), matching this
//! project's existing precedent of provider-specific code living outside
//! the `CloudProvider` trait when it's about the shim's own host rather
//! than a resource the trait manages.

use serde::Deserialize;
use std::time::Duration;

const DEFAULT_METADATA_URL: &str = "http://169.254.169.254/metadata/v1.json";

#[derive(Debug, Deserialize)]
struct MetadataResponse {
    network: NetworkMetadata,
}

#[derive(Debug, Deserialize)]
struct NetworkMetadata {
    interfaces: Vec<InterfaceMetadata>,
}

#[derive(Debug, Deserialize)]
struct InterfaceMetadata {
    #[serde(rename = "type")]
    interface_type: String,
    ip_addresses: Vec<IpAddressMetadata>,
}

#[derive(Debug, Deserialize)]
struct IpAddressMetadata {
    family: String,
    address: String,
}

/// The shim's own current public IPv4, or `None` if it can't be
/// determined -- not running on UpCloud at all (local dev, CI, a future
/// non-UpCloud deployment), the metadata service is unreachable, or the
/// response doesn't contain a public IPv4 interface. Always non-fatal:
/// callers (see `main.rs`) treat `None` the same safe way the UpCloud
/// connectivity check's own failure is treated -- log and continue, never
/// crash the shim over it. A worker VM created with no known shim IP
/// simply gets no inbound-SSH-allow rule at all (fully closed, the safe
/// direction to fail in), not a crash.
pub async fn own_public_ipv4() -> Option<String> {
    own_public_ipv4_from(DEFAULT_METADATA_URL).await
}

async fn own_public_ipv4_from(url: &str) -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok()?;

    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(
                "could not reach UpCloud metadata service to determine the shim's own public \
                 IP (this is expected when not running on UpCloud, e.g. local dev/CI): {err}"
            );
            return None;
        }
    };

    let body: MetadataResponse = match response.json().await {
        Ok(body) => body,
        Err(err) => {
            tracing::warn!("UpCloud metadata service returned an unparseable response: {err}");
            return None;
        }
    };

    let ip = body
        .network
        .interfaces
        .into_iter()
        .find(|iface| iface.interface_type == "public")
        .and_then(|iface| {
            iface
                .ip_addresses
                .into_iter()
                .find(|addr| addr.family == "IPv4")
        })
        .map(|addr| addr.address);

    if ip.is_none() {
        tracing::warn!(
            "UpCloud metadata service response had no public IPv4 interface; worker VMs will \
             get no inbound-SSH-allow rule"
        );
    }
    ip
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use serde_json::json;
    use std::net::{SocketAddr, TcpListener as StdTcpListener};

    async fn mock_metadata_server(app: axum::Router) -> String {
        let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = std_listener.local_addr().unwrap();
        std_listener.set_nonblocking(true).unwrap();

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("http://{addr}/metadata/v1.json")
    }

    #[tokio::test]
    async fn test_finds_public_ipv4_among_multiple_interfaces() {
        let app = axum::Router::new().route(
            "/metadata/v1.json",
            axum::routing::get(|| async {
                Json(json!({"network": {"interfaces": [
                    {"type": "utility", "ip_addresses": [{"family": "IPv4", "address": "10.4.0.1"}]},
                    {"type": "public", "ip_addresses": [
                        {"family": "IPv6", "address": "2a04::1"},
                        {"family": "IPv4", "address": "87.58.155.231"}
                    ]}
                ]}}))
            }),
        );
        let url = mock_metadata_server(app).await;

        assert_eq!(
            own_public_ipv4_from(&url).await.as_deref(),
            Some("87.58.155.231")
        );
    }

    #[tokio::test]
    async fn test_no_public_interface_returns_none() {
        let app = axum::Router::new().route(
            "/metadata/v1.json",
            axum::routing::get(|| async {
                Json(json!({"network": {"interfaces": [
                    {"type": "utility", "ip_addresses": [{"family": "IPv4", "address": "10.4.0.1"}]}
                ]}}))
            }),
        );
        let url = mock_metadata_server(app).await;

        assert_eq!(own_public_ipv4_from(&url).await, None);
    }

    #[tokio::test]
    async fn test_unreachable_metadata_service_returns_none() {
        // Nothing listening on this port -- simulates "not running on
        // UpCloud at all" without needing to fake a connection refusal.
        assert_eq!(
            own_public_ipv4_from("http://127.0.0.1:1/metadata/v1.json").await,
            None
        );
    }
}
