//! Admission checks for `CronJob`/standalone `Job` create (Phase 5;
//! generalized in Phase 13 for Jobs to reuse) -- policy rejections in
//! the same shape a real cluster's `ValidatingAdmissionPolicy`/webhook
//! would produce, not bespoke error formats. See
//! docs/IMPLEMENTATION_PLAN.md Phase 5.
//!
//! Every check here takes the pod-template-level spec directly (what a
//! standalone `Job`'s own `.spec` already is, and what a `CronJob`'s
//! `.spec.jobTemplate.spec` unwraps to) plus `field_prefix`, the
//! caller's own field path for that spec in *its* rejection messages
//! (`"spec"` for a Job, `"spec.jobTemplate.spec"` for a CronJob) --
//! one validation implementation, two thin call sites
//! (`api::cronjob::create_cronjob`/`api::job::create_job`), not two
//! copies of the same three checks.

use crate::k8s_status;
use crate::volumes::{self, StorageTier};
use crate::workload;
use axum::response::Response;
use serde_json::Value as JsonValue;

/// `{field_prefix}.activeDeadlineSeconds` must be set. Optional in the
/// real Kubernetes API, but the shim needs a hard worst-case runtime
/// bound for every job to make the budget guard (Phase 14) and deadline
/// enforcement (Phase 11) meaningful -- so it's required via policy, the
/// same way a real cluster's admission webhook would reject a policy
/// violation. Resolves Open Question 9.
///
/// Returns the rejection response directly (rather than `Result<(),
/// Response>`) since `Response` is too large for clippy's
/// `result_large_err` lint to accept as an `Err` variant -- `Option` isn't
/// subject to that lint and reads just as clearly at the call site.
pub fn require_active_deadline_seconds(
    pod_template_spec: &JsonValue,
    field_prefix: &str,
) -> Option<Response> {
    let has_deadline = pod_template_spec
        .pointer("/activeDeadlineSeconds")
        .is_some();

    if has_deadline {
        None
    } else {
        Some(k8s_status::status_error(
            axum::http::StatusCode::FORBIDDEN,
            "Forbidden",
            format!(
                "admission webhook \"kube-shim.brawer.ch/require-active-deadline\" denied the \
                 request: {field_prefix}.activeDeadlineSeconds must be set (bounds the \
                 job's worst-case cost against the budget guard)"
            ),
        ))
    }
}

/// Validates `storageClassName` on every `ephemeral` volume in the pod
/// template, rejecting the first one that isn't a known storage tier
/// (`kube-shim-standard`/`kube-shim-fast`) -- the same idiomatic shape a
/// real cluster uses for "this enum value isn't one of the supported
/// ones" (HTTP 422, `reason: Invalid`), distinct from the 403 `Forbidden`
/// above (that's a cluster *policy* denying an otherwise-valid request;
/// this is a plain field-value validation failure). Volumes with no
/// `ephemeral` entry (or no `volumes` at all) are left alone -- this check
/// only concerns itself with the field it's actually validating.
pub fn validate_ephemeral_volume_storage_classes(
    object_description: &str,
    pod_template_spec: &JsonValue,
    field_prefix: &str,
) -> Option<Response> {
    let volumes = pod_template_spec
        .pointer("/template/spec/volumes")
        .and_then(JsonValue::as_array)?;

    for volume in volumes {
        let Some(pvc_spec) = volume.pointer("/ephemeral/volumeClaimTemplate/spec") else {
            continue;
        };
        let storage_class_name = pvc_spec.get("storageClassName").and_then(JsonValue::as_str);

        if let Err(unknown) = StorageTier::parse(storage_class_name) {
            let field = format!(
                "{field_prefix}.template.spec.volumes[].ephemeral.\
                 volumeClaimTemplate.spec.storageClassName"
            );
            return Some(k8s_status::invalid_field_value(
                object_description,
                &field,
                unknown,
            ));
        }
    }

    None
}

/// Validates `resources.limits.cpu`/`.memory` on the pod template's
/// first container -- real Kubernetes validates the `Quantity` type at
/// the OpenAPI-schema level and rejects a malformed value at admission
/// time (a 400/422 before the object is ever persisted), it never
/// silently accepts the manifest and drops the limit at runtime. Since
/// `cloud_init::generate` passes these straight through to `podman run
/// --cpus`/`--memory` (closing the gap where limits weren't honored at
/// all), skipping this check would mean kube-shim accepts a manifest a
/// real cluster would reject outright -- the opposite of what honoring
/// `limits` in the first place was meant to de-risk.
pub fn validate_resource_limits(
    object_description: &str,
    pod_template_spec: &JsonValue,
    field_prefix: &str,
) -> Option<Response> {
    let limits = pod_template_spec.pointer("/template/spec/containers/0/resources/limits")?;
    let base_field = format!("{field_prefix}.template.spec.containers[0].resources.limits");

    if let Some(cpu) = limits.get("cpu").and_then(JsonValue::as_str) {
        if workload::parse_cpu_millicores(cpu).is_err() {
            return Some(k8s_status::invalid_field_value(
                object_description,
                &format!("{base_field}.cpu"),
                cpu,
            ));
        }
    }
    if let Some(memory) = limits.get("memory").and_then(JsonValue::as_str) {
        if volumes::parse_storage_quantity_mb(memory).is_err() {
            return Some(k8s_status::invalid_field_value(
                object_description,
                &format!("{base_field}.memory"),
                memory,
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::json;

    /// CronJob's own field prefix -- matches `create_cronjob`'s real
    /// call site, used throughout these tests unless a test is
    /// specifically about the prefix itself.
    const CRONJOB_PREFIX: &str = "spec.jobTemplate.spec";

    async fn response_message(response: Response) -> String {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&body).unwrap();
        json["message"].as_str().unwrap().to_string()
    }

    #[test]
    fn test_active_deadline_seconds_present_accepted() {
        let spec = json!({"activeDeadlineSeconds": 3600});
        assert!(require_active_deadline_seconds(&spec, CRONJOB_PREFIX).is_none());
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_missing_rejected() {
        let spec = json!({});
        let response =
            require_active_deadline_seconds(&spec, CRONJOB_PREFIX).expect("must be rejected");
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
        let message = response_message(response).await;
        assert!(message.contains("activeDeadlineSeconds"));
        assert!(message.contains("spec.jobTemplate.spec.activeDeadlineSeconds"));
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_missing_uses_job_field_prefix() {
        // A standalone Job's own call site (api::job::create_job) passes
        // "spec" directly, not "spec.jobTemplate.spec" -- the rejection
        // message must reflect whichever prefix the caller actually
        // passed, not hardcode the CronJob one.
        let spec = json!({});
        let response = require_active_deadline_seconds(&spec, "spec").expect("must be rejected");
        let message = response_message(response).await;
        assert!(message.contains("spec.activeDeadlineSeconds"));
        assert!(!message.contains("jobTemplate"));
    }

    #[test]
    fn test_ephemeral_volume_no_volumes_accepted() {
        let spec = json!({"template": {"spec": {"containers": []}}});
        assert!(
            validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec, CRONJOB_PREFIX)
                .is_none()
        );
    }

    #[test]
    fn test_ephemeral_volume_omitted_storage_class_accepted() {
        let spec = json!({"template": {"spec": {
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "resources": {"requests": {"storage": "250Gi"}}
            }}}}]
        }}});
        assert!(
            validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec, CRONJOB_PREFIX)
                .is_none()
        );
    }

    #[test]
    fn test_ephemeral_volume_known_storage_classes_accepted() {
        for class in ["kube-shim-standard", "kube-shim-fast"] {
            let spec = json!({"template": {"spec": {
                "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                    "storageClassName": class
                }}}}]
            }}});
            assert!(
                validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec, CRONJOB_PREFIX)
                    .is_none(),
                "{class} should be accepted"
            );
        }
    }

    #[tokio::test]
    async fn test_ephemeral_volume_unknown_storage_class_rejected() {
        let spec = json!({"template": {"spec": {
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "storageClassName": "premium-ultra-disk"
            }}}}]
        }}});
        let response =
            validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec, CRONJOB_PREFIX)
                .expect("must be rejected");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNPROCESSABLE_ENTITY
        );
        let message = response_message(response).await;
        assert!(message.contains("storageClassName"));
        assert!(message.contains("premium-ultra-disk"));
        assert!(message.contains("spec.jobTemplate.spec.template.spec.volumes"));
    }

    #[tokio::test]
    async fn test_ephemeral_volume_unknown_storage_class_uses_job_field_prefix() {
        let spec = json!({"template": {"spec": {
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "storageClassName": "premium-ultra-disk"
            }}}}]
        }}});
        let response = validate_ephemeral_volume_storage_classes("Job \"x\"", &spec, "spec")
            .expect("must be rejected");
        let message = response_message(response).await;
        assert!(message.contains("spec.template.spec.volumes"));
        assert!(!message.contains("jobTemplate"));
    }

    #[test]
    fn test_non_ephemeral_volumes_are_ignored() {
        // A secret-mount volume (or any other non-ephemeral kind) must not
        // trip storageClassName validation at all.
        let spec = json!({"template": {"spec": {
            "volumes": [{"name": "creds", "secret": {"secretName": "osmdiffs-s3-credentials"}}]
        }}});
        assert!(
            validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec, CRONJOB_PREFIX)
                .is_none()
        );
    }

    #[test]
    fn test_resource_limits_no_limits_set_accepted() {
        let spec = json!({"template": {"spec": {
            "containers": [{"image": "x"}]
        }}});
        assert!(validate_resource_limits("CronJob \"x\"", &spec, CRONJOB_PREFIX).is_none());
    }

    #[test]
    fn test_resource_limits_valid_values_accepted() {
        let spec = json!({"template": {"spec": {
            "containers": [{"image": "x", "resources": {"limits": {"cpu": "500m", "memory": "512Mi"}}}]
        }}});
        assert!(validate_resource_limits("CronJob \"x\"", &spec, CRONJOB_PREFIX).is_none());
    }

    #[tokio::test]
    async fn test_resource_limits_malformed_cpu_rejected() {
        // Real Kubernetes rejects a malformed Quantity at admission time
        // (OpenAPI schema validation) -- it never silently accepts the
        // manifest and just drops the limit at runtime.
        let spec = json!({"template": {"spec": {
            "containers": [{"image": "x", "resources": {"limits": {"cpu": "not-a-quantity"}}}]
        }}});
        let response = validate_resource_limits("CronJob \"x\"", &spec, CRONJOB_PREFIX)
            .expect("must be rejected");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNPROCESSABLE_ENTITY
        );
        let message = response_message(response).await;
        assert!(message
            .contains("spec.jobTemplate.spec.template.spec.containers[0].resources.limits.cpu"));
        assert!(message.contains("not-a-quantity"));
    }

    #[tokio::test]
    async fn test_resource_limits_malformed_memory_rejected() {
        let spec = json!({"template": {"spec": {
            "containers": [{"image": "x", "resources": {"limits": {"memory": "lots"}}}]
        }}});
        let response = validate_resource_limits("CronJob \"x\"", &spec, CRONJOB_PREFIX)
            .expect("must be rejected");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNPROCESSABLE_ENTITY
        );
        assert!(response_message(response)
            .await
            .contains("resources.limits.memory"));
    }

    #[tokio::test]
    async fn test_resource_limits_malformed_cpu_uses_job_field_prefix() {
        let spec = json!({"template": {"spec": {
            "containers": [{"image": "x", "resources": {"limits": {"cpu": "not-a-quantity"}}}]
        }}});
        let response =
            validate_resource_limits("Job \"x\"", &spec, "spec").expect("must be rejected");
        let message = response_message(response).await;
        assert!(message.contains("spec.template.spec.containers[0].resources.limits.cpu"));
        assert!(!message.contains("jobTemplate"));
    }
}
