//! Volume (storage) operations. Every endpoint shape verified against
//! UpCloud's own API reference (`developers.upcloud.com/1.3/9-storages`)
//! directly -- see docs/IMPLEMENTATION_PLAN.md Phase 7.

use super::UpCloudProvider;
use crate::providers::{CreateVolumeRequest, ProviderError, Volume};
use reqwest::Method;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct CreateStorageBody {
    storage: StorageParams,
}

#[derive(Serialize)]
struct StorageParams {
    size: u32,
    tier: String,
    title: String,
    zone: String,
}

#[derive(Deserialize)]
struct StorageEnvelope {
    storage: StorageObject,
}

#[derive(Deserialize)]
struct StorageObject {
    uuid: String,
    size: u32,
    tier: String,
    title: String,
    zone: String,
}

pub(super) async fn create_volume(
    provider: &UpCloudProvider,
    req: CreateVolumeRequest,
) -> Result<Volume, ProviderError> {
    let body = CreateStorageBody {
        storage: StorageParams {
            size: req.size_gb,
            tier: req.tier,
            title: req.title,
            zone: req.zone,
        },
    };
    let response: StorageEnvelope = provider
        .send_json(provider.request(Method::POST, "/storage").json(&body))
        .await?;
    Ok(Volume {
        id: response.storage.uuid,
        size_gb: response.storage.size,
        tier: response.storage.tier,
        title: response.storage.title,
        zone: response.storage.zone,
    })
}

pub(super) async fn delete_volume(
    provider: &UpCloudProvider,
    volume_id: &str,
) -> Result<(), ProviderError> {
    provider
        .send_no_content(provider.request(Method::DELETE, &format!("/storage/{volume_id}")))
        .await
}

#[derive(Serialize)]
struct StorageDeviceRefBody {
    storage_device: StorageDeviceRef,
}

#[derive(Serialize)]
struct StorageDeviceRef {
    storage: String,
}

pub(super) async fn attach_volume(
    provider: &UpCloudProvider,
    server_id: &str,
    volume_id: &str,
) -> Result<(), ProviderError> {
    let body = StorageDeviceRefBody {
        storage_device: StorageDeviceRef {
            storage: volume_id.to_string(),
        },
    };
    let _: serde_json::Value = provider
        .send_json(
            provider
                .request(Method::POST, &format!("/server/{server_id}/storage/attach"))
                .json(&body),
        )
        .await?;
    Ok(())
}

pub(super) async fn detach_volume(
    provider: &UpCloudProvider,
    server_id: &str,
    volume_id: &str,
) -> Result<(), ProviderError> {
    let body = StorageDeviceRefBody {
        storage_device: StorageDeviceRef {
            storage: volume_id.to_string(),
        },
    };
    let _: serde_json::Value = provider
        .send_json(
            provider
                .request(Method::POST, &format!("/server/{server_id}/storage/detach"))
                .json(&body),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::mock_server;
    use crate::providers::{CloudProvider, CreateVolumeRequest};
    use axum::{extract::Path, routing::post, Json};
    use serde_json::json;

    #[tokio::test]
    async fn test_create_volume_parses_response() {
        let app = axum::Router::new().route(
            "/1.3/storage",
            post(|| async {
                Json(json!({
                    "storage": {
                        "uuid": "01abc",
                        "size": 10,
                        "tier": "maxiops",
                        "title": "test-volume",
                        "zone": "de-fra1"
                    }
                }))
            }),
        );
        let provider = mock_server(app).await;

        let volume = provider
            .create_volume(CreateVolumeRequest {
                size_gb: 10,
                tier: "maxiops".to_string(),
                title: "test-volume".to_string(),
                zone: "de-fra1".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(volume.id, "01abc");
        assert_eq!(volume.size_gb, 10);
        assert_eq!(volume.tier, "maxiops");
        assert_eq!(volume.zone, "de-fra1");
    }

    #[tokio::test]
    async fn test_delete_volume_no_content() {
        let app = axum::Router::new().route(
            "/1.3/storage/:uuid",
            axum::routing::delete(|Path(_uuid): Path<String>| async {
                axum::http::StatusCode::NO_CONTENT
            }),
        );
        let provider = mock_server(app).await;

        provider.delete_volume("01abc").await.unwrap();
    }

    #[tokio::test]
    async fn test_attach_and_detach_volume() {
        let app = axum::Router::new()
            .route(
                "/1.3/server/:uuid/storage/attach",
                post(|| async { Json(json!({"server": {}})) }),
            )
            .route(
                "/1.3/server/:uuid/storage/detach",
                post(|| async { Json(json!({"server": {}})) }),
            );
        let provider = mock_server(app).await;

        provider.attach_volume("srv1", "vol1").await.unwrap();
        provider.detach_volume("srv1", "vol1").await.unwrap();
    }
}
