//! Server operations. Endpoint/request shapes verified against UpCloud's
//! own API reference and, for `create_server`, against a real live call
//! made while researching UpCloud during an earlier phase of this project
//! (see docs/IMPLEMENTATION_PLAN.md Phase 7 for the async-creation
//! finding this module's `get_server` exists to support).

use super::UpCloudProvider;
use crate::providers::{CreateServerRequest, ProviderError, Server};
use reqwest::Method;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct CreateServerBody {
    server: ServerParams,
}

#[derive(Serialize)]
struct ServerParams {
    zone: String,
    title: String,
    hostname: String,
    plan: String,
    metadata: String,
    login_user: LoginUser,
    storage_devices: StorageDevices,
}

#[derive(Serialize)]
struct LoginUser {
    username: String,
    ssh_keys: SshKeys,
}

#[derive(Serialize)]
struct SshKeys {
    ssh_key: Vec<String>,
}

#[derive(Serialize)]
struct StorageDevices {
    storage_device: Vec<StorageDeviceSpec>,
}

#[derive(Serialize)]
struct StorageDeviceSpec {
    action: &'static str,
    storage: String,
    title: String,
    size: u32,
    tier: &'static str,
}

#[derive(Deserialize)]
struct ServerEnvelope {
    server: ServerObject,
}

#[derive(Deserialize)]
struct ServerObject {
    uuid: String,
    title: String,
    state: String,
    #[serde(default)]
    ip_addresses: Option<IpAddresses>,
}

#[derive(Deserialize)]
struct IpAddresses {
    ip_address: Vec<IpAddress>,
}

#[derive(Deserialize)]
struct IpAddress {
    access: String,
    family: String,
    address: String,
}

fn extract_public_ips(ip_addresses: &Option<IpAddresses>) -> (Option<String>, Option<String>) {
    let mut ipv4 = None;
    let mut ipv6 = None;
    if let Some(ip_addresses) = ip_addresses {
        for ip in &ip_addresses.ip_address {
            if ip.access != "public" {
                continue;
            }
            match ip.family.as_str() {
                "IPv4" => ipv4 = Some(ip.address.clone()),
                "IPv6" => ipv6 = Some(ip.address.clone()),
                _ => {}
            }
        }
    }
    (ipv4, ipv6)
}

fn to_server(object: ServerObject) -> Server {
    let (public_ipv4, public_ipv6) = extract_public_ips(&object.ip_addresses);
    Server {
        id: object.uuid,
        title: object.title,
        state: object.state,
        public_ipv4,
        public_ipv6,
    }
}

pub(super) async fn create_server(
    provider: &UpCloudProvider,
    req: CreateServerRequest,
) -> Result<Server, ProviderError> {
    let body = CreateServerBody {
        server: ServerParams {
            zone: req.zone,
            title: req.title.clone(),
            hostname: req.hostname,
            plan: req.plan,
            metadata: "yes".to_string(),
            login_user: LoginUser {
                username: "root".to_string(),
                ssh_keys: SshKeys {
                    ssh_key: req.ssh_public_keys,
                },
            },
            storage_devices: StorageDevices {
                storage_device: vec![StorageDeviceSpec {
                    action: "clone",
                    storage: req.template_uuid,
                    title: format!("{}-os", req.title),
                    size: req.boot_disk_size_gb,
                    tier: "standard",
                }],
            },
        },
    };
    let response: ServerEnvelope = provider
        .send_json(provider.request(Method::POST, "/server").json(&body))
        .await?;
    Ok(to_server(response.server))
}

pub(super) async fn get_server(
    provider: &UpCloudProvider,
    server_id: &str,
) -> Result<Server, ProviderError> {
    let response: ServerEnvelope = provider
        .send_json(provider.request(Method::GET, &format!("/server/{server_id}")))
        .await?;
    Ok(to_server(response.server))
}

pub(super) async fn delete_server(
    provider: &UpCloudProvider,
    server_id: &str,
) -> Result<(), ProviderError> {
    // storages=0: this project always deletes ephemeral volumes through
    // its own separate delete_volume() call, not implicitly here -- see
    // CloudProvider's own docs on why volume and server lifecycle stay
    // independent operations.
    provider
        .send_no_content(
            provider.request(Method::DELETE, &format!("/server/{server_id}?storages=0")),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::super::tests::mock_server;
    use crate::providers::{CloudProvider, CreateServerRequest};
    use axum::{extract::Path, routing::get, Json};
    use serde_json::json;

    fn sample_request() -> CreateServerRequest {
        CreateServerRequest {
            title: "kube-shim-worker-test".to_string(),
            hostname: "kube-shim-worker-test".to_string(),
            zone: "de-fra1".to_string(),
            plan: "DEV-1xCPU-1GB-10GB".to_string(),
            template_uuid: "01000000-0000-4000-8000-000030240200".to_string(),
            boot_disk_size_gb: 10,
            ssh_public_keys: vec!["ssh-ed25519 AAAA... test".to_string()],
        }
    }

    #[tokio::test]
    async fn test_create_server_parses_response_including_public_ips() {
        let app = axum::Router::new().route(
            "/1.3/server",
            axum::routing::post(|| async {
                Json(json!({
                    "server": {
                        "uuid": "003a02c7",
                        "title": "kube-shim-worker-test",
                        "state": "maintenance",
                        "ip_addresses": {"ip_address": [
                            {"access": "utility", "family": "IPv4", "address": "10.4.0.1"},
                            {"access": "public", "family": "IPv4", "address": "94.237.90.144"},
                            {"access": "public", "family": "IPv6", "address": "2a04::1"}
                        ]}
                    }
                }))
            }),
        );
        let provider = mock_server(app).await;

        let server = provider.create_server(sample_request()).await.unwrap();

        assert_eq!(server.id, "003a02c7");
        assert_eq!(server.state, "maintenance");
        assert_eq!(server.public_ipv4.as_deref(), Some("94.237.90.144"));
        assert_eq!(server.public_ipv6.as_deref(), Some("2a04::1"));
    }

    #[tokio::test]
    async fn test_get_server_for_polling() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            get(|Path(_uuid): Path<String>| async {
                Json(json!({"server": {"uuid": "003a02c7", "title": "t", "state": "started"}}))
            }),
        );
        let provider = mock_server(app).await;

        let server = provider.get_server("003a02c7").await.unwrap();
        assert_eq!(server.state, "started");
    }

    #[tokio::test]
    async fn test_delete_server_no_content() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid",
            axum::routing::delete(|Path(_uuid): Path<String>| async {
                axum::http::StatusCode::NO_CONTENT
            }),
        );
        let provider = mock_server(app).await;

        provider.delete_server("003a02c7").await.unwrap();
    }

    #[tokio::test]
    async fn test_authentication_failure_is_reported_distinctly() {
        let app = axum::Router::new().route(
            "/1.3/server",
            axum::routing::post(|| async { axum::http::StatusCode::UNAUTHORIZED }),
        );
        let provider = mock_server(app).await;

        let err = provider.create_server(sample_request()).await.unwrap_err();
        assert!(matches!(
            err,
            crate::providers::ProviderError::AuthenticationFailed(_)
        ));
    }
}
