//! Admission checks for `CronJob` create/update -- policy rejections in
//! the same shape a real cluster's `ValidatingAdmissionPolicy`/webhook
//! would produce, not bespoke error formats. See
//! docs/IMPLEMENTATION_PLAN.md Phase 5.

use crate::k8s_status;
use crate::volumes::StorageTier;
use axum::response::Response;
use serde_json::Value as JsonValue;

/// `spec.jobTemplate.spec.activeDeadlineSeconds` must be set. Optional in
/// the real Kubernetes API, but the shim needs a hard worst-case runtime
/// bound for every job to make the budget guard (Phase 13) and deadline
/// enforcement (Phase 11) meaningful -- so it's required via policy, the
/// same way a real cluster's admission webhook would reject a policy
/// violation. Resolves Open Question 9.
///
/// Returns the rejection response directly (rather than `Result<(),
/// Response>`) since `Response` is too large for clippy's
/// `result_large_err` lint to accept as an `Err` variant -- `Option` isn't
/// subject to that lint and reads just as clearly at the call site.
pub fn require_active_deadline_seconds(spec: &JsonValue) -> Option<Response> {
    let has_deadline = spec
        .pointer("/jobTemplate/spec/activeDeadlineSeconds")
        .is_some();

    if has_deadline {
        None
    } else {
        Some(k8s_status::status_error(
            axum::http::StatusCode::FORBIDDEN,
            "Forbidden",
            "admission webhook \"kube-shim.io/require-active-deadline\" denied the request: \
             spec.jobTemplate.spec.activeDeadlineSeconds must be set (bounds the job's \
             worst-case cost against the budget guard)",
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
    spec: &JsonValue,
) -> Option<Response> {
    let volumes = spec
        .pointer("/jobTemplate/spec/template/spec/volumes")
        .and_then(JsonValue::as_array)?;

    for volume in volumes {
        let Some(pvc_spec) = volume.pointer("/ephemeral/volumeClaimTemplate/spec") else {
            continue;
        };
        let storage_class_name = pvc_spec.get("storageClassName").and_then(JsonValue::as_str);

        if let Err(unknown) = StorageTier::parse(storage_class_name) {
            let field = "spec.jobTemplate.spec.template.spec.volumes[].ephemeral.\
                         volumeClaimTemplate.spec.storageClassName";
            return Some(k8s_status::invalid_field_value(
                object_description,
                field,
                unknown,
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

    async fn response_message(response: Response) -> String {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&body).unwrap();
        json["message"].as_str().unwrap().to_string()
    }

    #[test]
    fn test_active_deadline_seconds_present_accepted() {
        let spec = json!({
            "schedule": "0 2 * * 0",
            "jobTemplate": {"spec": {"activeDeadlineSeconds": 3600}}
        });
        assert!(require_active_deadline_seconds(&spec).is_none());
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_missing_rejected() {
        let spec = json!({
            "schedule": "0 2 * * 0",
            "jobTemplate": {"spec": {}}
        });
        let response = require_active_deadline_seconds(&spec).expect("must be rejected");
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
        assert!(response_message(response)
            .await
            .contains("activeDeadlineSeconds"));
    }

    #[tokio::test]
    async fn test_active_deadline_seconds_missing_job_template_rejected() {
        // No jobTemplate at all -- must not panic navigating the JSON path.
        let spec = json!({"schedule": "0 2 * * 0"});
        let response = require_active_deadline_seconds(&spec).expect("must be rejected");
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn test_ephemeral_volume_no_volumes_accepted() {
        let spec = json!({"jobTemplate": {"spec": {"template": {"spec": {"containers": []}}}}});
        assert!(validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec).is_none());
    }

    #[test]
    fn test_ephemeral_volume_omitted_storage_class_accepted() {
        let spec = json!({"jobTemplate": {"spec": {"template": {"spec": {
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "resources": {"requests": {"storage": "250Gi"}}
            }}}}]
        }}}}});
        assert!(validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec).is_none());
    }

    #[test]
    fn test_ephemeral_volume_known_storage_classes_accepted() {
        for class in ["kube-shim-standard", "kube-shim-fast"] {
            let spec = json!({"jobTemplate": {"spec": {"template": {"spec": {
                "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                    "storageClassName": class
                }}}}]
            }}}}});
            assert!(
                validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec).is_none(),
                "{class} should be accepted"
            );
        }
    }

    #[tokio::test]
    async fn test_ephemeral_volume_unknown_storage_class_rejected() {
        let spec = json!({"jobTemplate": {"spec": {"template": {"spec": {
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "storageClassName": "premium-ultra-disk"
            }}}}]
        }}}}});
        let response = validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec)
            .expect("must be rejected");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::UNPROCESSABLE_ENTITY
        );
        let message = response_message(response).await;
        assert!(message.contains("storageClassName"));
        assert!(message.contains("premium-ultra-disk"));
    }

    #[test]
    fn test_non_ephemeral_volumes_are_ignored() {
        // A secret-mount volume (or any other non-ephemeral kind) must not
        // trip storageClassName validation at all.
        let spec = json!({"jobTemplate": {"spec": {"template": {"spec": {
            "volumes": [{"name": "creds", "secret": {"secretName": "osmdiffs-s3-credentials"}}]
        }}}}});
        assert!(validate_ephemeral_volume_storage_classes("CronJob \"x\"", &spec).is_none());
    }
}
