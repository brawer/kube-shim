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
    /// Hardcoded "on", not configurable: every worker VM must have
    /// UpCloud's own cloud firewall active from the moment it exists, or
    /// `create_firewall_rules` (Phase 9) has nothing to enforce -- verified
    /// hands-on that a freshly created server otherwise comes back with
    /// `"firewall": "off"` despite UpCloud's own docs describing "on" as
    /// the *modify*-endpoint's default (that default only applies to a
    /// PUT that omits the field, not to server creation).
    firewall: &'static str,
    login_user: LoginUser,
    storage_devices: StorageDevices,
    /// UpCloud's own "server setup script" -- see `CreateServerRequest`'s
    /// own docs on why this is plain text, not base64/a URL.
    user_data: String,
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
            firewall: "on",
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
            user_data: req.user_data,
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
    // storages=1: deletes the server's *own* boot disk along with it --
    // real finding from Phase 9, correcting Phase 7's original guess of
    // storages=0. The boot disk is created together with the server
    // (`create_server`'s own `storage_devices`) and never referenced by
    // anything else, so nothing would ever clean it up otherwise. This is
    // safe for the job's separate *ephemeral* scratch volume too: that one
    // is a genuinely independent resource with its own tracked lifecycle
    // (`job_volumes`, `delete_volume`), but the reconciliation pipeline
    // (`reconcile::job`) always runs `VolumeDetaching` -- which detaches
    // *and deletes* that volume -- before `VMTerminating` calls this, so
    // by the time a real delete_server call happens, nothing but the boot
    // disk is left attached for storages=1 to catch.
    provider
        .send_no_content(
            provider.request(Method::DELETE, &format!("/server/{server_id}?storages=1")),
        )
        .await
}

#[derive(Deserialize)]
struct ServerListEnvelope {
    servers: ServerList,
}

#[derive(Deserialize)]
struct ServerList {
    server: Vec<ServerListEntry>,
}

/// `GET /server`'s list entries are a different (smaller) shape than a
/// single server's own representation -- no `ip_addresses` at all, so
/// orphan scanning (Phase 9's actual caller) only gets id/title/state/zone
/// out of this, which is all it needs to decide what's untracked.
#[derive(Deserialize)]
struct ServerListEntry {
    uuid: String,
    title: String,
    state: String,
    zone: String,
}

pub(super) async fn list_servers(
    provider: &UpCloudProvider,
    zone: &str,
) -> Result<Vec<Server>, ProviderError> {
    let response: ServerListEnvelope = provider
        .send_json(provider.request(Method::GET, "/server"))
        .await?;

    Ok(response
        .servers
        .server
        .into_iter()
        .filter(|s| s.zone == zone)
        .map(|s| Server {
            id: s.uuid,
            title: s.title,
            state: s.state,
            public_ipv4: None,
            public_ipv6: None,
        })
        .collect())
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
            user_data: "#!/bin/bash\necho hi\n".to_string(),
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

    #[tokio::test]
    async fn test_create_server_request_sets_firewall_on_and_user_data() {
        let app = axum::Router::new().route(
            "/1.3/server",
            axum::routing::post(|body: String| async move {
                assert!(body.contains("\"firewall\":\"on\""));
                assert!(body.contains("echo hi"));
                Json(json!({"server": {"uuid": "003a02c7", "title": "t", "state": "maintenance"}}))
            }),
        );
        let provider = mock_server(app).await;

        provider.create_server(sample_request()).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_servers_filters_by_zone() {
        let app = axum::Router::new().route(
            "/1.3/server",
            get(|| async {
                Json(json!({"servers": {"server": [
                    {"uuid": "a", "title": "kube-shim-worker-a", "state": "started", "zone": "de-fra1"},
                    {"uuid": "b", "title": "kube-shim-worker-b", "state": "started", "zone": "fi-hel1"}
                ]}}))
            }),
        );
        let provider = mock_server(app).await;

        let servers = provider.list_servers("de-fra1").await.unwrap();

        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].id, "a");
    }
}
