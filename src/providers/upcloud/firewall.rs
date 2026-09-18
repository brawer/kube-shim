//! Firewall rule operations. Endpoint shapes verified both against
//! UpCloud's own API reference and against a real live call made while
//! standing up kube-shim.brawer.ch (see docs/IMPLEMENTATION_PLAN.md
//! Phase 4/7) -- including the finding that rule application lags the API
//! call by roughly 1-2 minutes, which is exactly why `list_firewall_rules`
//! exists as a separate primitive from `create_firewall_rules`: Phase 9
//! builds the actual wait/verify step on top of it.

use super::UpCloudProvider;
use crate::providers::{
    FirewallAction, FirewallDirection, FirewallFamily, FirewallRule, ProviderError,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct FirewallRuleBody {
    firewall_rule: FirewallRuleParams,
}

#[derive(Serialize)]
struct FirewallRuleParams {
    direction: &'static str,
    action: &'static str,
    family: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_address_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_address_end: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_port_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_port_end: Option<String>,
}

fn direction_str(direction: FirewallDirection) -> &'static str {
    match direction {
        FirewallDirection::In => "in",
        FirewallDirection::Out => "out",
    }
}

fn action_str(action: FirewallAction) -> &'static str {
    match action {
        FirewallAction::Accept => "accept",
        FirewallAction::Drop => "drop",
    }
}

fn family_str(family: FirewallFamily) -> &'static str {
    match family {
        FirewallFamily::Ipv4 => "IPv4",
        FirewallFamily::Ipv6 => "IPv6",
    }
}

fn to_params(rule: &FirewallRule) -> FirewallRuleParams {
    // A single exact address is expressed as a one-address range --
    // see FirewallRule's own docs for why the public API stays simpler
    // than UpCloud's full range feature.
    let (source_address_start, source_address_end) = match &rule.source_address {
        Some(address) => (Some(address.clone()), Some(address.clone())),
        None => (None, None),
    };
    FirewallRuleParams {
        direction: direction_str(rule.direction),
        action: action_str(rule.action),
        family: family_str(rule.family),
        protocol: rule.protocol.clone(),
        source_address_start,
        source_address_end,
        destination_port_start: rule.destination_port_start.clone(),
        destination_port_end: rule.destination_port_end.clone(),
    }
}

/// Adds each rule in `rules`, in order, via one `POST` per rule --
/// UpCloud's create endpoint only ever adds a single rule per call, there
/// is no bulk-replace. Appended without an explicit `position` (letting
/// UpCloud place them after whatever's already there): a brand new worker
/// VM (Phase 9's actual use case) starts with no rules of its own, so
/// there's nothing to insert *before* yet -- revisit if that assumption
/// turns out wrong once Phase 9 tests this against a real fresh server.
pub(super) async fn create_firewall_rules(
    provider: &UpCloudProvider,
    server_id: &str,
    rules: &[FirewallRule],
) -> Result<(), ProviderError> {
    for rule in rules {
        let body = FirewallRuleBody {
            firewall_rule: to_params(rule),
        };
        let _: serde_json::Value = provider
            .send_json(
                provider
                    .request(Method::POST, &format!("/server/{server_id}/firewall_rule"))
                    .json(&body),
            )
            .await?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct FirewallRuleListEnvelope {
    firewall_rules: FirewallRuleList,
}

#[derive(Deserialize)]
struct FirewallRuleList {
    firewall_rule: Vec<FirewallRuleObject>,
}

#[derive(Deserialize)]
struct FirewallRuleObject {
    direction: String,
    action: String,
    family: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    source_address_start: String,
    #[serde(default)]
    destination_port_start: String,
    #[serde(default)]
    destination_port_end: String,
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

pub(super) async fn list_firewall_rules(
    provider: &UpCloudProvider,
    server_id: &str,
) -> Result<Vec<FirewallRule>, ProviderError> {
    let response: FirewallRuleListEnvelope = provider
        .send_json(provider.request(Method::GET, &format!("/server/{server_id}/firewall_rule")))
        .await?;

    Ok(response
        .firewall_rules
        .firewall_rule
        .into_iter()
        .map(|rule| FirewallRule {
            direction: if rule.direction == "in" {
                FirewallDirection::In
            } else {
                FirewallDirection::Out
            },
            action: if rule.action == "accept" {
                FirewallAction::Accept
            } else {
                FirewallAction::Drop
            },
            family: if rule.family == "IPv4" {
                FirewallFamily::Ipv4
            } else {
                FirewallFamily::Ipv6
            },
            protocol: non_empty(rule.protocol),
            source_address: non_empty(rule.source_address_start),
            destination_port_start: non_empty(rule.destination_port_start),
            destination_port_end: non_empty(rule.destination_port_end),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::tests::mock_server;
    use crate::providers::{
        CloudProvider, FirewallAction, FirewallDirection, FirewallFamily, FirewallRule,
    };
    use axum::{routing::post, Json};
    use serde_json::json;

    fn ssh_from_shim_rule() -> FirewallRule {
        FirewallRule {
            direction: FirewallDirection::In,
            action: FirewallAction::Accept,
            family: FirewallFamily::Ipv4,
            protocol: Some("tcp".to_string()),
            source_address: Some("203.0.113.5".to_string()),
            destination_port_start: Some("22".to_string()),
            destination_port_end: Some("22".to_string()),
        }
    }

    #[tokio::test]
    async fn test_create_firewall_rules_posts_one_per_rule() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            post(|| async { Json(json!({"firewall_rule": {}})) }),
        );
        let provider = mock_server(app).await;

        provider
            .create_firewall_rules("srv1", &[ssh_from_shim_rule()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_list_firewall_rules_parses_response() {
        let app = axum::Router::new().route(
            "/1.3/server/:uuid/firewall_rule",
            axum::routing::get(|| async {
                Json(json!({"firewall_rules": {"firewall_rule": [
                    {
                        "direction": "in", "action": "accept", "family": "IPv4",
                        "protocol": "tcp", "source_address_start": "203.0.113.5",
                        "source_address_end": "203.0.113.5",
                        "destination_port_start": "22", "destination_port_end": "22"
                    },
                    {
                        "direction": "in", "action": "drop", "family": "IPv4",
                        "protocol": "", "source_address_start": "", "source_address_end": "",
                        "destination_port_start": "", "destination_port_end": ""
                    }
                ]}}))
            }),
        );
        let provider = mock_server(app).await;

        let rules = provider.list_firewall_rules("srv1").await.unwrap();

        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].action, FirewallAction::Accept);
        assert_eq!(rules[0].source_address.as_deref(), Some("203.0.113.5"));
        assert_eq!(rules[1].action, FirewallAction::Drop);
        assert_eq!(rules[1].source_address, None);
    }
}
