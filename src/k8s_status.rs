//! Builds Kubernetes-shaped `Status` error responses (the same JSON shape
//! and HTTP status codes a real Kubernetes API server returns for auth
//! failures, admission denials, and validation errors), so `kubectl` and
//! Terraform's `kubernetes_provider` surface these exactly as they would
//! for a real cluster instead of some bespoke error format.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Status {
    kind: &'static str,
    #[serde(rename = "apiVersion")]
    api_version: &'static str,
    /// Real Kubernetes Status objects always carry a metadata field, even
    /// though it's empty for an error response. Included for fidelity with
    /// what client-go (and therefore kubectl/Terraform) expects to parse.
    metadata: serde_json::Value,
    status: &'static str,
    message: String,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<StatusDetails>,
    code: u16,
}

/// Real Kubernetes `Status.details` for a field-validation failure --
/// `causes` is what lets a programmatic consumer point at exactly which
/// field was wrong, rather than only having the human-readable `message`.
#[derive(Debug, Serialize)]
struct StatusDetails {
    causes: Vec<StatusCause>,
}

#[derive(Debug, Serialize)]
struct StatusCause {
    reason: &'static str,
    message: String,
    field: String,
}

/// Builds a `kind: Status` error response with the given HTTP status code
/// and Kubernetes `reason` (e.g. `"Unauthorized"`, `"Forbidden"`,
/// `"Invalid"`). `message` is a human-readable explanation.
pub fn status_error(
    code: StatusCode,
    reason: &'static str,
    message: impl Into<String>,
) -> axum::response::Response {
    status_error_with_details(code, reason, message, None)
}

fn status_error_with_details(
    code: StatusCode,
    reason: &'static str,
    message: impl Into<String>,
    details: Option<StatusDetails>,
) -> axum::response::Response {
    let body = Status {
        kind: "Status",
        api_version: "v1",
        metadata: serde_json::json!({}),
        status: "Failure",
        message: message.into(),
        reason,
        details,
        code: code.as_u16(),
    };
    (code, Json(body)).into_response()
}

/// 401 Unauthorized: the request has no (or invalid) authentication
/// credentials at all. Distinct from `Forbidden` (403), which means the
/// credentials were valid but the request is denied anyway.
pub fn unauthorized(message: impl Into<String>) -> axum::response::Response {
    status_error(StatusCode::UNAUTHORIZED, "Unauthorized", message)
}

/// 403 Forbidden, `reason: Forbidden`: a policy check (not a field-
/// validation error -- see `invalid_field_value` for that) rejects an
/// otherwise well-formed request. Message shape matches a real
/// cluster's native `ValidatingAdmissionPolicy` denial exactly
/// (`ValidatingAdmissionPolicy '<policy>' with binding '<binding>'
/// denied request: <reason>`) -- verified hands-on against the real
/// Kubernetes docs and multiple real examples, not assumed. Shared by
/// every policy-style check in this project (`src/admission.rs`,
/// `api::job::create_job`'s budget check) so they all name their own
/// policy consistently rather than each hand-rolling this string.
/// `binding_name` defaults to `"{policy_name}-binding"` -- this
/// project has no real separate binding object the way a real cluster
/// does, but the real message format always includes one, so this
/// invents a plausible one rather than quietly dropping half the
/// real shape.
pub fn policy_denied(policy_name: &str, reason: impl Into<String>) -> axum::response::Response {
    status_error(
        StatusCode::FORBIDDEN,
        "Forbidden",
        format!(
            "ValidatingAdmissionPolicy '{policy_name}' with binding '{policy_name}-binding' \
             denied request: {}",
            reason.into()
        ),
    )
}

/// 404 Not Found, `reason: NotFound`: no object of this kind exists with
/// this name/namespace -- the same shape a real cluster returns for
/// `kubectl get`/`kubectl logs` against a resource that doesn't exist.
pub fn not_found(kind: &str, name: &str) -> axum::response::Response {
    status_error(
        StatusCode::NOT_FOUND,
        "NotFound",
        format!("{kind} \"{name}\" not found"),
    )
}

/// 409 Conflict, `reason: AlreadyExists`: a `create` whose name collides
/// with an existing object in the same namespace -- the same shape a
/// real cluster returns for this (Phase 13: a standalone `Job`'s name,
/// unlike a `CronJob`-spawned run's own timestamp-suffixed one, is
/// whatever the caller gave it, so this can genuinely happen).
pub fn already_exists(kind: &str, name: &str) -> axum::response::Response {
    status_error(
        StatusCode::CONFLICT,
        "AlreadyExists",
        format!("{kind} \"{name}\" already exists"),
    )
}

/// 422 Unprocessable Entity, `reason: Invalid`: a field's value isn't one
/// of the values this API supports for it -- the same shape a real cluster
/// uses for an unsupported enum-style field value (e.g. an unknown
/// `storageClassName`), distinct from `Forbidden` (403), which is a
/// cluster *policy* denying an otherwise well-formed request.
pub fn invalid_field_value(
    object_description: impl Into<String>,
    field: &str,
    value: &str,
) -> axum::response::Response {
    let object_description = object_description.into();
    let cause_message = format!("Unsupported value: \"{value}\"");
    status_error_with_details(
        StatusCode::UNPROCESSABLE_ENTITY,
        "Invalid",
        format!("{object_description} is invalid: {field}: {cause_message}"),
        Some(StatusDetails {
            causes: vec![StatusCause {
                reason: "FieldValueNotSupported",
                message: cause_message,
                field: field.to_string(),
            }],
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn test_unauthorized_shape() {
        let response = unauthorized("authentication required");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["kind"], "Status");
        assert_eq!(json["apiVersion"], "v1");
        assert_eq!(json["status"], "Failure");
        assert_eq!(json["reason"], "Unauthorized");
        assert_eq!(json["code"], 401);
        assert_eq!(json["message"], "authentication required");
        assert!(json["metadata"].is_object());
    }

    #[tokio::test]
    async fn test_policy_denied_shape() {
        let response = policy_denied(
            "kube-shim.brawer.ch/require-active-deadline",
            "spec.activeDeadlineSeconds must be set",
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["reason"], "Forbidden");
        assert_eq!(json["code"], 403);
        // Exact real ValidatingAdmissionPolicy denial shape (verified
        // hands-on against the real Kubernetes docs and multiple real
        // examples) -- single quotes, "denied request" with no "the".
        assert_eq!(
            json["message"],
            "ValidatingAdmissionPolicy 'kube-shim.brawer.ch/require-active-deadline' with \
             binding 'kube-shim.brawer.ch/require-active-deadline-binding' denied request: \
             spec.activeDeadlineSeconds must be set"
        );
    }

    #[tokio::test]
    async fn test_not_found_shape() {
        let response = not_found("Pod", "osmdiffs-weekly-123");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["reason"], "NotFound");
        assert_eq!(json["code"], 404);
        assert!(json["message"]
            .as_str()
            .unwrap()
            .contains("osmdiffs-weekly-123"));
    }

    #[tokio::test]
    async fn test_already_exists_shape() {
        let response = already_exists("Job", "hello-world-test");
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["reason"], "AlreadyExists");
        assert_eq!(json["code"], 409);
        assert!(json["message"]
            .as_str()
            .unwrap()
            .contains("hello-world-test"));
    }

    #[tokio::test]
    async fn test_status_error_generic() {
        let response = status_error(StatusCode::FORBIDDEN, "Forbidden", "denied");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["reason"], "Forbidden");
        assert_eq!(json["code"], 403);
    }

    #[tokio::test]
    async fn test_invalid_field_value_shape() {
        let response = invalid_field_value(
            "CronJob \"osmdiffs-weekly\"",
            "spec.storageClassName",
            "bogus",
        );
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["kind"], "Status");
        assert_eq!(json["reason"], "Invalid");
        assert_eq!(json["code"], 422);
        assert!(json["message"]
            .as_str()
            .unwrap()
            .contains("spec.storageClassName"));

        let cause = &json["details"]["causes"][0];
        assert_eq!(cause["reason"], "FieldValueNotSupported");
        assert_eq!(cause["field"], "spec.storageClassName");
        assert!(cause["message"].as_str().unwrap().contains("bogus"));
    }
}
