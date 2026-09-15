//! Bearer-token authentication middleware for the authenticated `:6443`
//! API. Every route behind this middleware requires a valid
//! `Authorization: Bearer <token>` header, checked against a list of
//! configured tokens (see `config::ApiToken`) so a token can be rotated
//! with an overlap window instead of one atomic cutover.

use crate::config::ApiToken;
use crate::k8s_status::unauthorized;
use axum::{
    extract::{Request, State},
    http::header,
    middleware::Next,
    response::Response,
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use subtle::{Choice, ConstantTimeEq};

/// Checks `presented` against every entry in `tokens`, in constant time
/// with respect to the token *contents*: every entry is compared
/// unconditionally (no short-circuit on the first match), and the
/// per-entry byte comparison itself is constant-time, so response timing
/// can't be used to learn how many leading bytes of a guess matched, or
/// which configured token (if any) it matched. Only non-expired entries
/// can match.
pub fn token_is_valid(tokens: &[ApiToken], presented: &str, now: DateTime<Utc>) -> bool {
    let presented_bytes = presented.as_bytes();
    let mut matched = Choice::from(0u8);

    for entry in tokens {
        let bytes_match = entry.token.as_bytes().ct_eq(presented_bytes);
        let not_expired = Choice::from(entry.is_valid_at(now) as u8);
        matched |= bytes_match & not_expired;
    }

    matched.into()
}

/// Axum middleware: requires `Authorization: Bearer <token>` on the
/// wrapped router, checked against the token list this middleware was
/// constructed with. Returns a standard Kubernetes 401 `Status` response
/// when the header is missing, malformed, or the token isn't currently
/// valid.
pub async fn require_bearer_token(
    State(tokens): State<Arc<Vec<ApiToken>>>,
    req: Request,
    next: Next,
) -> Response {
    let presented_token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    match presented_token {
        Some(token) if token_is_valid(&tokens, token, Utc::now()) => next.run(req).await,
        _ => unauthorized("Unauthorized"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn token(value: &str) -> ApiToken {
        ApiToken {
            token: value.to_string(),
            expires_at: None,
        }
    }

    fn expired_token(value: &str, now: DateTime<Utc>) -> ApiToken {
        ApiToken {
            token: value.to_string(),
            expires_at: Some(now - Duration::days(1)),
        }
    }

    #[test]
    fn test_valid_token_matches() {
        let tokens = vec![token("secret-a"), token("secret-b")];
        assert!(token_is_valid(&tokens, "secret-a", Utc::now()));
        assert!(token_is_valid(&tokens, "secret-b", Utc::now()));
    }

    #[test]
    fn test_wrong_token_rejected() {
        let tokens = vec![token("secret-a")];
        assert!(!token_is_valid(&tokens, "wrong", Utc::now()));
    }

    #[test]
    fn test_empty_token_list_rejects_everything() {
        let tokens: Vec<ApiToken> = vec![];
        assert!(!token_is_valid(&tokens, "anything", Utc::now()));
    }

    #[test]
    fn test_empty_presented_token_rejected() {
        let tokens = vec![token("secret-a")];
        assert!(!token_is_valid(&tokens, "", Utc::now()));
    }

    #[test]
    fn test_expired_token_rejected() {
        let now = Utc::now();
        let tokens = vec![expired_token("secret-a", now)];
        assert!(!token_is_valid(&tokens, "secret-a", now));
    }

    #[test]
    fn test_expired_token_alongside_valid_one() {
        let now = Utc::now();
        let tokens = vec![expired_token("old", now), token("new")];
        assert!(!token_is_valid(&tokens, "old", now));
        assert!(token_is_valid(&tokens, "new", now));
    }

    #[test]
    fn test_token_with_different_length_rejected() {
        let tokens = vec![token("short")];
        assert!(!token_is_valid(&tokens, "a-much-longer-guess", Utc::now()));
        assert!(!token_is_valid(&tokens, "shor", Utc::now()));
    }

    #[test]
    fn test_prefix_of_valid_token_is_not_valid() {
        let tokens = vec![token("secret-token-value")];
        assert!(!token_is_valid(&tokens, "secret-token", Utc::now()));
    }
}
