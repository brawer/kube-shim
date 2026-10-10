pub mod budget;
pub mod cost_report;
pub mod cronjob;
pub mod events;
pub mod job;
pub mod logs;
pub mod metrics;
pub mod nodes;
pub mod pods;
pub mod secret;

#[cfg(test)]
mod tests;

use axum::{response::IntoResponse, Json};
use serde_json::json;

/// The bare, version-less root (distinct from `/api/v1` below) --
/// `kubectl`'s own discovery client always calls this plus `/apis`
/// (`discovery_apis_root`) *before* ever requesting a specific group/
/// version's resource list, even for a group it already knows the exact
/// path for (verified hands-on: `kubectl top nodes -v=8` against this
/// project without this endpoint existing calls `GET /api` and `GET
/// /apis`, gets 404 on both, and gives up with "the server could not
/// find the requested resource" without ever trying `/apis/
/// metrics.k8s.io/v1beta1/nodes` at all).
pub async fn discovery_root() -> impl IntoResponse {
    Json(json!({
        "kind": "APIVersions",
        "versions": ["v1"],
        "serverAddressByClientCIDRs": []
    }))
}

/// The bare `/apis` root (distinct from `/apis/batch/v1` and `/apis/
/// metrics.k8s.io/v1beta1` below) -- see `discovery_root`'s own docs for
/// why `kubectl` won't even attempt a group-specific call without this
/// existing first.
pub async fn discovery_apis_root() -> impl IntoResponse {
    Json(json!({
        "kind": "APIGroupList",
        "groups": [
            {
                "name": "batch",
                "versions": [{"groupVersion": "batch/v1", "version": "v1"}],
                "preferredVersion": {"groupVersion": "batch/v1", "version": "v1"}
            },
            {
                "name": "metrics.k8s.io",
                "versions": [{"groupVersion": "metrics.k8s.io/v1beta1", "version": "v1beta1"}],
                "preferredVersion": {"groupVersion": "metrics.k8s.io/v1beta1", "version": "v1beta1"}
            },
            {
                "name": "cost.kube-shim.brawer.ch",
                "versions": [{"groupVersion": "cost.kube-shim.brawer.ch/v1", "version": "v1"}],
                "preferredVersion": {"groupVersion": "cost.kube-shim.brawer.ch/v1", "version": "v1"}
            }
        ]
    }))
}

/// `cost.kube-shim.brawer.ch/v1` (Phase 14a) -- a kube-shim-specific
/// extension API group, the same convention real custom Kubernetes
/// extensions use (e.g. `metrics.k8s.io` above) for something with no
/// upstream Kubernetes equivalent. Only a CSV report today
/// (`api::cost_report`); Phase 14b's budget settings/top-up endpoints
/// join this same group once they exist.
pub async fn discovery_cost_v1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "cost.kube-shim.brawer.ch/v1",
        "resources": [
            {
                "name": "report",
                "namespaced": false,
                "kind": "CostReport",
                "verbs": ["get"]
            },
            {
                "name": "settings",
                "namespaced": false,
                "kind": "CostSettings",
                "verbs": ["get", "patch"]
            },
            {
                "name": "budget/topup",
                "namespaced": false,
                "kind": "BudgetTopUp",
                "verbs": ["create"]
            }
        ]
    });
    Json(response)
}

pub async fn discovery_v1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "v1",
        "resources": [
            {
                "name": "secrets",
                "singularName": "secret",
                "namespaced": true,
                "kind": "Secret",
                "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]
            },
            {
                "name": "pods",
                "singularName": "pod",
                "namespaced": true,
                "kind": "Pod",
                // Only "get" (api::pods, Phase 12) and the /log
                // subresource (api::logs, Phase 10) -- there's no
                // standalone Pod *list* in this project (a job run *is*
                // its own pod, 1:1, for its whole lifecycle, but nothing
                // yet needs to list all of them as Pods specifically),
                // so this stays honest about what's actually implemented
                // rather than claiming a verb with no handler behind it.
                "verbs": ["get"]
            },
            {
                "name": "events",
                "singularName": "event",
                "namespaced": true,
                "kind": "Event",
                "verbs": ["get", "list"]
            },
            {
                "name": "nodes",
                "singularName": "node",
                // Cluster-scoped, same as a real Node -- there's no
                // namespace a worker VM belongs to.
                "namespaced": false,
                "kind": "Node",
                "verbs": ["get", "list"]
            }
        ]
    });
    Json(response)
}

/// `metrics.k8s.io/v1beta1` (Phase 12) -- what `kubectl top` discovers
/// before calling the actual `nodes`/`pods` endpoints (`api::metrics`).
pub async fn discovery_metrics_v1beta1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "metrics.k8s.io/v1beta1",
        "resources": [
            {
                "name": "nodes",
                "singularName": "nodemetrics",
                "namespaced": false,
                "kind": "NodeMetrics",
                "verbs": ["get", "list"]
            },
            {
                "name": "pods",
                "singularName": "podmetrics",
                "namespaced": true,
                "kind": "PodMetrics",
                "verbs": ["get", "list"]
            }
        ]
    });
    Json(response)
}

pub async fn discovery_batch_v1() -> impl IntoResponse {
    let response = json!({
        "kind": "APIResourceList",
        "groupVersion": "batch/v1",
        "resources": [
            {
                "name": "cronjobs",
                "singularName": "cronjob",
                "namespaced": true,
                "kind": "CronJob",
                "verbs": ["create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"]
            },
            {
                "name": "jobs",
                "singularName": "job",
                "namespaced": true,
                "kind": "Job",
                // No "patch"/"update"/"watch" -- same honesty-about-what's-
                // implemented convention api::pods's own discovery entry
                // already established (Phase 12): there's no Job update
                // handler, and watch needs a long-lived streaming
                // connection nothing here supports yet.
                "verbs": ["create", "delete", "deletecollection", "get", "list"]
            }
        ]
    });
    Json(response)
}

pub async fn health() -> impl IntoResponse {
    Json(json!({
        "status": "healthy"
    }))
}
