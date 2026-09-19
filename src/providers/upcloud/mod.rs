//! `UpCloudProvider`: a hand-rolled REST client on `reqwest` (no official
//! UpCloud Rust SDK exists) implementing `CloudProvider` against the real
//! UpCloud API (`https://api.upcloud.com/1.3`). Every endpoint shape used
//! here was verified against UpCloud's own API reference directly, not
//! guessed -- see docs/IMPLEMENTATION_PLAN.md Phase 7.

mod firewall;
mod pricing;
mod servers;
mod volumes;

use crate::providers::{
    CloudProvider, CreateServerRequest, CreateVolumeRequest, FirewallRule, PriceEntry,
    ProviderError, Server, Volume,
};
use async_trait::async_trait;
use reqwest::{Method, RequestBuilder, StatusCode};
use serde::de::DeserializeOwned;

const DEFAULT_BASE_URL: &str = "https://api.upcloud.com/1.3";

pub struct UpCloudProvider {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl UpCloudProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self::with_base_url(token, DEFAULT_BASE_URL)
    }

    /// Only ever overridden in tests, to point at a local mock server
    /// instead of the real UpCloud API.
    pub fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            token: token.into(),
        }
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        builder: RequestBuilder,
    ) -> Result<T, ProviderError> {
        let response = builder.send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(error_for_status(
                status,
                response.text().await.unwrap_or_default(),
            ));
        }
        response.json::<T>().await.map_err(ProviderError::Request)
    }

    /// For requests whose success response has no body worth decoding
    /// (UpCloud's `DELETE` endpoints return 204 No Content).
    async fn send_no_content(&self, builder: RequestBuilder) -> Result<(), ProviderError> {
        let response = builder.send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(error_for_status(
                status,
                response.text().await.unwrap_or_default(),
            ));
        }
        Ok(())
    }

    /// Best-effort connectivity/auth check (`GET /1.3/account`) -- called
    /// once at startup (see `main.rs`), logged as a warning on failure,
    /// never fatal: nothing in Phase 7 actually depends on UpCloud
    /// working yet (real operations don't happen until Phase 8), and the
    /// already-deployed `kube-shim.brawer.ch` config still has
    /// `upcloud.token = "REPLACE_ME"` -- a hard failure here would crash-
    /// loop that instance on its next auto-update for no operational
    /// reason.
    pub async fn check_connectivity(&self) -> Result<(), ProviderError> {
        let _: serde_json::Value = self
            .send_json(self.request(Method::GET, "/account"))
            .await?;
        Ok(())
    }
}

fn error_for_status(status: StatusCode, body: String) -> ProviderError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            ProviderError::AuthenticationFailed(body)
        }
        StatusCode::NOT_FOUND => ProviderError::NotFound(body),
        _ => ProviderError::Api {
            status: status.as_u16(),
            message: body,
        },
    }
}

#[async_trait]
impl CloudProvider for UpCloudProvider {
    async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume, ProviderError> {
        volumes::create_volume(self, req).await
    }

    async fn delete_volume(&self, volume_id: &str) -> Result<(), ProviderError> {
        volumes::delete_volume(self, volume_id).await
    }

    async fn list_volumes(&self, zone: &str) -> Result<Vec<Volume>, ProviderError> {
        volumes::list_volumes(self, zone).await
    }

    async fn attach_volume(&self, server_id: &str, volume_id: &str) -> Result<(), ProviderError> {
        volumes::attach_volume(self, server_id, volume_id).await
    }

    async fn detach_volume(&self, server_id: &str, volume_id: &str) -> Result<(), ProviderError> {
        volumes::detach_volume(self, server_id, volume_id).await
    }

    async fn create_server(&self, req: CreateServerRequest) -> Result<Server, ProviderError> {
        servers::create_server(self, req).await
    }

    async fn get_server(&self, server_id: &str) -> Result<Server, ProviderError> {
        servers::get_server(self, server_id).await
    }

    async fn delete_server(&self, server_id: &str) -> Result<(), ProviderError> {
        servers::delete_server(self, server_id).await
    }

    async fn create_firewall_rules(
        &self,
        server_id: &str,
        rules: &[FirewallRule],
    ) -> Result<(), ProviderError> {
        firewall::create_firewall_rules(self, server_id, rules).await
    }

    async fn list_firewall_rules(
        &self,
        server_id: &str,
    ) -> Result<Vec<FirewallRule>, ProviderError> {
        firewall::list_firewall_rules(self, server_id).await
    }

    async fn get_pricing(&self, zone: &str, price_key: &str) -> Result<PriceEntry, ProviderError> {
        pricing::get_pricing(self, zone, price_key).await
    }
}

/// Shared test helper: every submodule's tests exercise the real HTTP
/// round trip (request building *and* response parsing) against a real
/// local server, not a hand-mocked `reqwest` layer -- the same pattern
/// `tests/tls_integration_test.rs` already uses for the shim's own
/// listener. Never touches the real UpCloud API.
#[cfg(test)]
pub(crate) mod tests {
    use super::UpCloudProvider;
    use std::net::{SocketAddr, TcpListener as StdTcpListener};

    pub(crate) async fn mock_server(app: axum::Router) -> UpCloudProvider {
        let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = std_listener.local_addr().unwrap();
        std_listener.set_nonblocking(true).unwrap();

        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        // Give the listener a moment to actually start accepting --
        // matches the same small sleep tls_integration_test.rs uses.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        UpCloudProvider::with_base_url("mock-token", format!("http://{addr}/1.3"))
    }
}
