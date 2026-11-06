//! Server-plan catalog (issue #85). `GET /1.3/plan` lists every plan
//! the account can order. UpCloud's docs say every plan is available in
//! every zone, with only the price differing per zone (`GET /1.3/price`,
//! see `pricing.rs`).

use super::UpCloudProvider;
use crate::providers::{PlanInfo, ProviderError};
use reqwest::Method;
use serde::Deserialize;

#[derive(Deserialize)]
struct PlansEnvelope {
    plans: Plans,
}

#[derive(Deserialize)]
struct Plans {
    plan: Vec<RawPlan>,
}

#[derive(Deserialize)]
struct RawPlan {
    name: String,
    core_number: u32,
    /// MB.
    memory_amount: u32,
    /// GB. Missing or 0 for plans without included storage.
    #[serde(default)]
    storage_size: Option<u32>,
    #[serde(default)]
    storage_tier: Option<String>,
    #[serde(default)]
    gpu_amount: Option<u32>,
    /// `"yes"`/`"no"`. Not in UpCloud's own docs example, but present in
    /// the live response; treated as current when missing.
    #[serde(default)]
    current_offering: Option<String>,
}

pub(super) async fn list_plans(provider: &UpCloudProvider) -> Result<Vec<PlanInfo>, ProviderError> {
    let response: PlansEnvelope = provider
        .send_json(provider.request(Method::GET, "/plan"))
        .await?;
    Ok(response
        .plans
        .plan
        .into_iter()
        .map(|p| PlanInfo {
            name: p.name,
            cpu_cores: p.core_number,
            memory_mb: p.memory_amount,
            storage_gb: p.storage_size.unwrap_or(0),
            storage_tier: p.storage_tier,
            gpu_count: p.gpu_amount.unwrap_or(0),
            current_offering: p.current_offering.as_deref() != Some("no"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::tests::mock_server;
    use crate::providers::{CloudProvider, PlanInfo};
    use axum::{routing::get, Json};
    use serde_json::json;

    #[tokio::test]
    async fn test_list_plans_parses_the_documented_response() {
        // Shape copied from UpCloud's own API docs ("List available
        // plans"), including a GPU plan with a null storage tier.
        let app = axum::Router::new().route(
            "/1.3/plan",
            get(|| async {
                Json(json!({
                    "plans": {"plan": [
                        {"core_number": 1, "memory_amount": 2048, "name": "1xCPU-2GB",
                         "public_traffic_out": 2048, "storage_size": 50, "storage_tier": "maxiops"},
                        {"core_number": 1, "memory_amount": 1024, "name": "DEV-1xCPU-1GB-10GB",
                         "storage_size": 10, "storage_tier": "standard", "current_offering": "no"},
                        {"core_number": 6, "memory_amount": 16384, "name": "CLOUDNATIVE-6xCPU-16GB",
                         "public_traffic_out": 1024, "storage_size": 0, "storage_tier": null},
                        {"core_number": 32, "memory_amount": 393216,
                         "name": "GPU-32xCPU-384GB-2xL40S", "public_traffic_out": 44,
                         "gpu_amount": 2, "storage_size": 0, "storage_tier": null,
                         "gpu_model": "NVIDIA L40S"}
                    ]}
                }))
            }),
        );
        let provider = mock_server(app).await;

        let plans = provider.list_plans().await.unwrap();
        assert_eq!(
            plans,
            vec![
                PlanInfo {
                    name: "1xCPU-2GB".to_string(),
                    cpu_cores: 1,
                    memory_mb: 2048,
                    storage_gb: 50,
                    storage_tier: Some("maxiops".to_string()),
                    gpu_count: 0,
                    current_offering: true,
                },
                PlanInfo {
                    name: "DEV-1xCPU-1GB-10GB".to_string(),
                    cpu_cores: 1,
                    memory_mb: 1024,
                    storage_gb: 10,
                    storage_tier: Some("standard".to_string()),
                    gpu_count: 0,
                    current_offering: false,
                },
                PlanInfo {
                    name: "CLOUDNATIVE-6xCPU-16GB".to_string(),
                    cpu_cores: 6,
                    memory_mb: 16384,
                    storage_gb: 0,
                    storage_tier: None,
                    gpu_count: 0,
                    current_offering: true,
                },
                PlanInfo {
                    name: "GPU-32xCPU-384GB-2xL40S".to_string(),
                    cpu_cores: 32,
                    memory_mb: 393216,
                    storage_gb: 0,
                    storage_tier: None,
                    gpu_count: 2,
                    current_offering: true,
                },
            ]
        );
    }
}
