//! A job container's environment: `env[]` (literal `value`s plus
//! `valueFrom.secretKeyRef`/`valueFrom.fieldRef`) and `envFrom[].secretRef`,
//! resolved to the final `NAME=VALUE` list the worker's `podman run`
//! gets, plus Kubernetes' own `$(VAR)` expansion for `command`/`args`
//! (issue #84).
//!
//! **Nothing is silently dropped.** An earlier version passed only
//! literal `value`s and skipped everything else, so a job whose
//! credentials came from a `Secret` still ran -- just without them. For
//! osmdiffs that meant a "successful" run that uploaded nothing (an
//! unset `<prefix>_ENDPOINT` means "don't upload"). Now:
//!
//! - an env source this project can't provide (`configMapKeyRef`,
//!   `resourceFieldRef`, `envFrom[].configMapRef`, a `fieldRef` path
//!   other than `metadata.name`/`metadata.namespace`) is rejected at
//!   admission (`validate`, called from `src/admission.rs`), before the
//!   CronJob/Job is ever stored;
//! - a missing `Secret` or key is a *runtime* error (`resolve`), since
//!   -- exactly as in Kubernetes -- the Secret may legitimately be
//!   created after the CronJob that references it. The job then never
//!   starts without it: `reconcile::job`'s `VMPending` handler keeps
//!   retrying (Kubernetes' own `CreateContainerConfigError` behavior)
//!   until the uniform stuck-state timeout fails the job. `optional:
//!   true` keeps its real meaning: a missing optional ref is skipped.
//!
//! Precedence follows Kubernetes: every `envFrom` source first (a later
//! source wins over an earlier one on a key clash), then `env[]` in
//! order, each entry overriding any earlier variable of the same name.
//!
//! `fieldRef: metadata.name` is the job run's own name: a job run *is*
//! its pod in this project (see `api::logs`'s docs on that 1:1 mapping),
//! so this is the same name `kubectl logs`/`kubectl describe pod` use.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::{Map, Value as JsonValue};
use sqlx::SqlitePool;
use std::collections::HashMap;

/// A container spec problem found at admission time: `field` is the
/// offending field's path relative to the container (e.g.
/// `env[0].valueFrom`), `value` the value being rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvSpecError {
    pub field: String,
    pub value: String,
}

fn spec_error(field: String, value: impl Into<String>) -> EnvSpecError {
    EnvSpecError {
        field,
        value: value.into(),
    }
}

const SUPPORTED_FIELD_PATHS: &[&str] = &["metadata.name", "metadata.namespace"];

/// Static checks on `env[]`/`envFrom[]` that don't need the database --
/// see the module docs for what's rejected here vs. at runtime.
pub fn validate(container: &JsonValue) -> Result<(), EnvSpecError> {
    if let Some(entries) = container.get("env").and_then(JsonValue::as_array) {
        for (i, entry) in entries.iter().enumerate() {
            validate_env_entry(entry, &format!("env[{i}]"))?;
        }
    }
    if let Some(sources) = container.get("envFrom").and_then(JsonValue::as_array) {
        for (i, source) in sources.iter().enumerate() {
            validate_env_from_source(source, &format!("envFrom[{i}]"))?;
        }
    }
    Ok(())
}

fn validate_env_name(name: Option<&JsonValue>, field: String) -> Result<(), EnvSpecError> {
    match name.and_then(JsonValue::as_str) {
        Some(name) if is_valid_env_name(name) => Ok(()),
        Some(name) => Err(spec_error(field, name)),
        None => Err(spec_error(field, "")),
    }
}

/// Kubernetes (since 1.32) accepts any printable ASCII except `=` in an
/// env var name -- the same rule here.
fn is_valid_env_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| (0x20..0x7f).contains(&b) && b != b'=')
}

fn validate_env_entry(entry: &JsonValue, field: &str) -> Result<(), EnvSpecError> {
    validate_env_name(entry.get("name"), format!("{field}.name"))?;
    let Some(value_from) = entry.get("valueFrom") else {
        return Ok(());
    };
    if entry
        .get("value")
        .and_then(JsonValue::as_str)
        .is_some_and(|v| !v.is_empty())
    {
        // Kubernetes rejects this combination too.
        return Err(spec_error(
            format!("{field}.valueFrom"),
            "may not be specified when `value` is not empty",
        ));
    }
    let Some(sources) = value_from.as_object() else {
        return Err(spec_error(
            format!("{field}.valueFrom"),
            value_from.to_string(),
        ));
    };
    let kinds: Vec<&String> = sources.keys().collect();
    match kinds.as_slice() {
        [kind] if kind.as_str() == "secretKeyRef" => {
            let secret_ref = &sources["secretKeyRef"];
            for key in ["name", "key"] {
                if non_empty_str(secret_ref, key).is_none() {
                    return Err(spec_error(
                        format!("{field}.valueFrom.secretKeyRef.{key}"),
                        "",
                    ));
                }
            }
            Ok(())
        }
        [kind] if kind.as_str() == "fieldRef" => {
            let path = sources["fieldRef"]
                .get("fieldPath")
                .and_then(JsonValue::as_str)
                .unwrap_or("");
            if SUPPORTED_FIELD_PATHS.contains(&path) {
                Ok(())
            } else {
                Err(spec_error(
                    format!("{field}.valueFrom.fieldRef.fieldPath"),
                    path,
                ))
            }
        }
        [kind] => Err(spec_error(format!("{field}.valueFrom"), kind.as_str())),
        _ => Err(spec_error(
            format!("{field}.valueFrom"),
            format!(
                "exactly one of secretKeyRef or fieldRef must be set, got {}",
                kinds.len()
            ),
        )),
    }
}

fn validate_env_from_source(source: &JsonValue, field: &str) -> Result<(), EnvSpecError> {
    let Some(map) = source.as_object() else {
        return Err(spec_error(field.to_string(), source.to_string()));
    };
    if let Some(prefix) = map.get("prefix") {
        let prefix = prefix.as_str().unwrap_or("=");
        if !prefix.is_empty() && !is_valid_env_name(prefix) {
            return Err(spec_error(format!("{field}.prefix"), prefix));
        }
    }
    let kinds: Vec<&String> = map.keys().filter(|k| k.as_str() != "prefix").collect();
    match kinds.as_slice() {
        [kind] if kind.as_str() == "secretRef" => {
            if non_empty_str(&map["secretRef"], "name").is_none() {
                return Err(spec_error(format!("{field}.secretRef.name"), ""));
            }
            Ok(())
        }
        [kind] => Err(spec_error(field.to_string(), kind.as_str())),
        _ => Err(spec_error(
            field.to_string(),
            format!("exactly one of secretRef must be set, got {}", kinds.len()),
        )),
    }
}

fn non_empty_str<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|s| !s.is_empty())
}

fn is_optional(reference: &JsonValue) -> bool {
    reference
        .get("optional")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

/// The pod-level fields a `fieldRef` can read.
pub struct PodFields<'a> {
    pub name: &'a str,
    pub namespace: &'a str,
}

/// Every Secret name `container` references, deduplicated.
fn referenced_secret_names(container: &JsonValue) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut add = |name: Option<&str>| {
        if let Some(name) = name {
            if !names.iter().any(|n| n == name) {
                names.push(name.to_string());
            }
        }
    };
    if let Some(sources) = container.get("envFrom").and_then(JsonValue::as_array) {
        for source in sources {
            add(source
                .pointer("/secretRef/name")
                .and_then(JsonValue::as_str));
        }
    }
    if let Some(entries) = container.get("env").and_then(JsonValue::as_array) {
        for entry in entries {
            add(entry
                .pointer("/valueFrom/secretKeyRef/name")
                .and_then(JsonValue::as_str));
        }
    }
    names
}

/// Resolves `containers[0]`'s full environment against the Secrets in
/// `pod.namespace`. Errors (missing Secret/key, malformed Secret data)
/// are meant to be shown to the user as-is.
pub async fn resolve(
    pool: &SqlitePool,
    pod: &PodFields<'_>,
    container: &JsonValue,
) -> Result<Vec<(String, String)>, String> {
    let mut secrets = HashMap::new();
    for name in referenced_secret_names(container) {
        let data: Option<String> =
            sqlx::query_scalar("SELECT data FROM secrets WHERE namespace = ? AND name = ?")
                .bind(pod.namespace)
                .bind(&name)
                .fetch_optional(pool)
                .await
                .map_err(|e| format!("reading Secret \"{name}\": {e}"))?;
        if let Some(data) = data {
            let mut data: JsonValue = serde_json::from_str(&data)
                .map_err(|e| format!("Secret \"{name}\" has unreadable data: {e}"))?;
            if data.is_null() {
                // A Secret created with no `data` at all: no keys.
                data = JsonValue::Object(Map::new());
            }
            secrets.insert(name, data);
        }
    }
    resolve_with_secrets(pod, container, &secrets)
}

/// `resolve`'s pure core, split out so it can be tested without a
/// database. `secrets` maps a Secret's name to its stored `data`.
pub fn resolve_with_secrets(
    pod: &PodFields<'_>,
    container: &JsonValue,
    secrets: &HashMap<String, JsonValue>,
) -> Result<Vec<(String, String)>, String> {
    // Defense in depth: a spec stored before admission checked this
    // must still fail loudly rather than be half-applied.
    validate(container).map_err(|e| {
        format!(
            "containers[0].{}: unsupported value \"{}\"",
            e.field, e.value
        )
    })?;

    let mut env: Vec<(String, String)> = Vec::new();

    if let Some(sources) = container.get("envFrom").and_then(JsonValue::as_array) {
        for source in sources {
            let secret_ref = &source["secretRef"];
            let secret_name = secret_ref["name"].as_str().unwrap_or_default();
            let prefix = source
                .get("prefix")
                .and_then(JsonValue::as_str)
                .unwrap_or("");
            let Some(data) = secrets.get(secret_name) else {
                if is_optional(secret_ref) {
                    continue;
                }
                return Err(secret_not_found(secret_name, pod.namespace));
            };
            let data = secret_data(secret_name, data)?;
            // Sorted, like Kubernetes' own iteration over a Secret's
            // keys, so the order is deterministic.
            let mut keys: Vec<&String> = data.keys().collect();
            keys.sort();
            for key in keys {
                let name = format!("{prefix}{key}");
                if !is_valid_env_name(&name) {
                    return Err(format!(
                        "Secret \"{secret_name}\" key \"{key}\" is not a valid environment \
                         variable name (via envFrom)"
                    ));
                }
                let value = decode_secret_value(secret_name, key, &data[key])?;
                set_var(&mut env, name, value);
            }
        }
    }

    if let Some(entries) = container.get("env").and_then(JsonValue::as_array) {
        for entry in entries {
            let name = entry["name"].as_str().unwrap_or_default().to_string();
            let value = if let Some(value_from) = entry.get("valueFrom") {
                if let Some(secret_ref) = value_from.get("secretKeyRef") {
                    let secret_name = secret_ref["name"].as_str().unwrap_or_default();
                    let key = secret_ref["key"].as_str().unwrap_or_default();
                    let Some(data) = secrets.get(secret_name) else {
                        if is_optional(secret_ref) {
                            continue;
                        }
                        return Err(secret_not_found(secret_name, pod.namespace));
                    };
                    let data = secret_data(secret_name, data)?;
                    match data.get(key) {
                        Some(raw) => decode_secret_value(secret_name, key, raw)?,
                        None if is_optional(secret_ref) => continue,
                        None => {
                            return Err(format!(
                                "couldn't find key {key} in Secret {}/{secret_name}",
                                pod.namespace
                            ))
                        }
                    }
                } else {
                    // validate() above guarantees fieldRef with a
                    // supported path.
                    match value_from
                        .pointer("/fieldRef/fieldPath")
                        .and_then(JsonValue::as_str)
                    {
                        Some("metadata.name") => pod.name.to_string(),
                        Some("metadata.namespace") => pod.namespace.to_string(),
                        other => return Err(format!("unsupported fieldRef {other:?}")),
                    }
                }
            } else {
                // Kubernetes expands $(VAR) in a literal value using the
                // variables defined before it; valueFrom values are not
                // expanded.
                let raw = entry.get("value").and_then(JsonValue::as_str).unwrap_or("");
                expand(raw, &env)
            };
            set_var(&mut env, name, value);
        }
    }

    Ok(env)
}

fn secret_not_found(name: &str, namespace: &str) -> String {
    // Same wording as the kubelet's own CreateContainerConfigError.
    format!("secret \"{name}\" not found in namespace \"{namespace}\"")
}

fn secret_data<'a>(name: &str, data: &'a JsonValue) -> Result<&'a Map<String, JsonValue>, String> {
    match data {
        JsonValue::Object(map) => Ok(map),
        _ => Err(format!("Secret \"{name}\" data is not an object")),
    }
}

/// A Secret's `data` values are base64-encoded, exactly as in
/// Kubernetes (what `kubectl create secret` and OpenTofu's
/// `kubernetes_secret` send).
fn decode_secret_value(secret: &str, key: &str, raw: &JsonValue) -> Result<String, String> {
    let encoded = raw
        .as_str()
        .ok_or_else(|| format!("Secret \"{secret}\" key \"{key}\" is not a string"))?;
    let bytes = BASE64.decode(encoded).map_err(|_| {
        format!("Secret \"{secret}\" key \"{key}\" is not base64-encoded (Secret data must be)")
    })?;
    let value = String::from_utf8(bytes)
        .map_err(|_| format!("Secret \"{secret}\" key \"{key}\" is not valid UTF-8"))?;
    if value.contains('\0') {
        return Err(format!(
            "Secret \"{secret}\" key \"{key}\" contains a NUL byte, which an environment \
             variable can't hold"
        ));
    }
    Ok(value)
}

fn set_var(env: &mut Vec<(String, String)>, name: String, value: String) {
    if let Some(existing) = env.iter_mut().find(|(n, _)| *n == name) {
        existing.1 = value;
    } else {
        env.push((name, value));
    }
}

/// Kubernetes' `$(VAR)` expansion, as used for `command`, `args` and
/// literal env values (a port of
/// `k8s.io/kubernetes/third_party/forked/golang/expansion`): `$(NAME)`
/// becomes `NAME`'s value if it's defined, and stays as-is otherwise;
/// `$$` is an escaped `$`; any other `$` is left alone.
pub fn expand(input: &str, env: &[(String, String)]) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 1..];
        if let Some(tail) = after.strip_prefix('$') {
            out.push('$');
            rest = tail;
        } else if let Some(inner) = after.strip_prefix('(') {
            match inner.find(')') {
                Some(end) => {
                    let name = &inner[..end];
                    match env.iter().find(|(n, _)| n == name) {
                        Some((_, value)) => out.push_str(value),
                        None => {
                            out.push_str("$(");
                            out.push_str(name);
                            out.push(')');
                        }
                    }
                    rest = &inner[end + 1..];
                }
                None => {
                    out.push_str("$(");
                    rest = inner;
                }
            }
        } else {
            out.push('$');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const POD: PodFields = PodFields {
        name: "osmdiffs-1762410000",
        namespace: "default",
    };

    fn b64(s: &str) -> JsonValue {
        JsonValue::String(BASE64.encode(s))
    }

    fn secrets() -> HashMap<String, JsonValue> {
        HashMap::from([
            (
                "osmdiffs-s3-credentials".to_string(),
                json!({
                    "PUBLIC_S3_ENDPOINT": b64("https://s3.example"),
                    "PUBLIC_S3_KEY": b64("pk"),
                    "INTERNAL_S3_KEY": b64("ik")
                }),
            ),
            (
                "other".to_string(),
                json!({"PUBLIC_S3_KEY": b64("from-other"), "PEM": b64("line1\nline2")}),
            ),
        ])
    }

    fn env_of(container: JsonValue) -> Result<Vec<(String, String)>, String> {
        resolve_with_secrets(&POD, &container, &secrets())
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_osmdiffs_snippet_from_issue_84() {
        let env = env_of(json!({
            "env": [{"name": "RUN_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}],
            "envFrom": [{"secretRef": {"name": "osmdiffs-s3-credentials"}}]
        }))
        .unwrap();
        assert_eq!(
            env,
            pairs(&[
                ("INTERNAL_S3_KEY", "ik"),
                ("PUBLIC_S3_ENDPOINT", "https://s3.example"),
                ("PUBLIC_S3_KEY", "pk"),
                ("RUN_ID", "osmdiffs-1762410000"),
            ])
        );
    }

    #[test]
    fn test_literal_values_still_work() {
        let env = env_of(json!({"env": [{"name": "A", "value": "1"}, {"name": "B", "value": ""}]}));
        assert_eq!(env.unwrap(), pairs(&[("A", "1"), ("B", "")]));
    }

    #[test]
    fn test_env_wins_over_env_from() {
        let env = env_of(json!({
            "env": [{"name": "PUBLIC_S3_KEY", "value": "explicit"}],
            "envFrom": [{"secretRef": {"name": "osmdiffs-s3-credentials"}}]
        }))
        .unwrap();
        assert!(env.contains(&("PUBLIC_S3_KEY".into(), "explicit".into())));
        assert_eq!(env.iter().filter(|(n, _)| n == "PUBLIC_S3_KEY").count(), 1);
    }

    #[test]
    fn test_later_env_from_source_wins() {
        let env = env_of(json!({
            "envFrom": [
                {"secretRef": {"name": "osmdiffs-s3-credentials"}},
                {"secretRef": {"name": "other"}}
            ]
        }))
        .unwrap();
        assert!(env.contains(&("PUBLIC_S3_KEY".into(), "from-other".into())));
    }

    #[test]
    fn test_env_from_prefix() {
        let env = env_of(json!({
            "envFrom": [{"prefix": "X_", "secretRef": {"name": "other"}}]
        }))
        .unwrap();
        assert_eq!(
            env,
            pairs(&[("X_PEM", "line1\nline2"), ("X_PUBLIC_S3_KEY", "from-other")])
        );
    }

    #[test]
    fn test_secret_key_ref_and_namespace_field_ref() {
        let env = env_of(json!({"env": [
            {"name": "KEY", "valueFrom": {"secretKeyRef": {"name": "other", "key": "PEM"}}},
            {"name": "NS", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}}
        ]}))
        .unwrap();
        assert_eq!(env, pairs(&[("KEY", "line1\nline2"), ("NS", "default")]));
    }

    #[test]
    fn test_missing_secret_is_an_error() {
        let err = env_of(json!({"envFrom": [{"secretRef": {"name": "nope"}}]})).unwrap_err();
        assert!(err.contains("secret \"nope\" not found"), "{err}");
        let err = env_of(json!({"env": [
            {"name": "A", "valueFrom": {"secretKeyRef": {"name": "nope", "key": "k"}}}
        ]}))
        .unwrap_err();
        assert!(err.contains("secret \"nope\" not found"), "{err}");
    }

    #[test]
    fn test_missing_key_is_an_error() {
        let err = env_of(json!({"env": [
            {"name": "A", "valueFrom": {"secretKeyRef": {"name": "other", "key": "nope"}}}
        ]}))
        .unwrap_err();
        assert!(err.contains("couldn't find key nope"), "{err}");
    }

    #[test]
    fn test_optional_refs_are_skipped_when_missing() {
        let env = env_of(json!({
            "env": [
                {"name": "A", "valueFrom": {"secretKeyRef": {"name": "nope", "key": "k", "optional": true}}},
                {"name": "B", "valueFrom": {"secretKeyRef": {"name": "other", "key": "nope", "optional": true}}}
            ],
            "envFrom": [{"secretRef": {"name": "nope", "optional": true}}]
        }));
        assert_eq!(env.unwrap(), vec![]);
    }

    #[test]
    fn test_non_base64_secret_value_is_an_error() {
        let secrets = HashMap::from([("s".to_string(), json!({"k": "not base64!"}))]);
        let container = json!({"envFrom": [{"secretRef": {"name": "s"}}]});
        let err = resolve_with_secrets(&POD, &container, &secrets).unwrap_err();
        assert!(err.contains("not base64-encoded"), "{err}");
    }

    #[test]
    fn test_literal_value_expands_earlier_vars() {
        let env = env_of(json!({"env": [
            {"name": "RUN_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": "DIR", "value": "/work/$(RUN_ID)/$(LATER)"},
            {"name": "LATER", "value": "x"}
        ]}))
        .unwrap();
        assert!(env.contains(&("DIR".into(), "/work/osmdiffs-1762410000/$(LATER)".into())));
    }

    #[test]
    fn test_unsupported_sources_are_rejected_by_validate() {
        for (container, field) in [
            (
                json!({"env": [{"name": "A", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}]}),
                "env[0].valueFrom",
            ),
            (
                json!({"env": [{"name": "A", "valueFrom": {"resourceFieldRef": {"resource": "limits.cpu"}}}]}),
                "env[0].valueFrom",
            ),
            (
                json!({"env": [{"name": "A", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}]}),
                "env[0].valueFrom.fieldRef.fieldPath",
            ),
            (
                json!({"envFrom": [{"configMapRef": {"name": "c"}}]}),
                "envFrom[0]",
            ),
            (
                json!({"env": [{"name": "A", "valueFrom": {"secretKeyRef": {"name": "s"}}}]}),
                "env[0].valueFrom.secretKeyRef.key",
            ),
            (
                json!({"env": [{"name": "A=B", "value": "x"}]}),
                "env[0].name",
            ),
            (
                json!({"env": [{"name": "A", "value": "x", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}]}),
                "env[0].valueFrom",
            ),
        ] {
            let err = validate(&container).expect_err(&container.to_string());
            assert_eq!(err.field, field, "{container}");
            // And resolve() refuses it too, rather than skipping it.
            assert!(env_of(container).is_err());
        }
    }

    #[test]
    fn test_validate_accepts_supported_sources() {
        assert!(validate(&json!({
            "env": [
                {"name": "A", "value": "1"},
                {"name": "B", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k", "optional": true}}},
                {"name": "C", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.name"}}}
            ],
            "envFrom": [{"prefix": "P_", "secretRef": {"name": "s"}}]
        }))
        .is_ok());
        assert!(validate(&json!({})).is_ok());
    }

    #[test]
    fn test_expand() {
        let env = pairs(&[("RUN_ID", "r1"), ("EMPTY", "")]);
        assert_eq!(expand("--run_id=$(RUN_ID)", &env), "--run_id=r1");
        assert_eq!(expand("$(RUN_ID)$(RUN_ID)", &env), "r1r1");
        assert_eq!(expand("$(EMPTY)x", &env), "x");
        assert_eq!(expand("$(UNDEFINED)", &env), "$(UNDEFINED)");
        assert_eq!(expand("$$(RUN_ID)", &env), "$(RUN_ID)");
        assert_eq!(expand("$$$(RUN_ID)", &env), "$r1");
        assert_eq!(expand("$RUN_ID ${RUN_ID}", &env), "$RUN_ID ${RUN_ID}");
        assert_eq!(expand("$(RUN_ID", &env), "$(RUN_ID");
        assert_eq!(expand("cost: 5$", &env), "cost: 5$");
        assert_eq!(expand("ä$(RUN_ID)ö$", &env), "är1ö$");
        assert_eq!(expand("", &env), "");
    }
}
