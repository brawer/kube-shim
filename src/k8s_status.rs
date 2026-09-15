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
    code: u16,
}

/// Builds a `kind: Status` error response with the given HTTP status code
/// and Kubernetes `reason` (e.g. `"Unauthorized"`, `"Forbidden"`,
/// `"Invalid"`). `message` is a human-readable explanation.
pub fn status_error(
    code: StatusCode,
    reason: &'static str,
    message: impl Into<String>,
) -> axum::response::Response {
    let body = Status {
        kind: "Status",
        api_version: "v1",
        metadata: serde_json::json!({}),
        status: "Failure",
        message: message.into(),
        reason,
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
    async fn test_status_error_generic() {
        let response = status_error(StatusCode::FORBIDDEN, "Forbidden", "denied");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["reason"], "Forbidden");
        assert_eq!(json["code"], 403);
    }
}
