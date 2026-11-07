//! Kubernetes protobuf request bodies (issue #106).
//!
//! client-go's generated clientset, which the `hashicorp/kubernetes`
//! OpenTofu provider uses, always sends built-in types as
//! `application/vnd.kubernetes.protobuf` and never falls back to JSON.
//! [`protobuf_bodies`] converts such a body to the JSON the same Go
//! object would have been written as, and hands it on with
//! `Content-Type: application/json`. Handlers and admission only ever
//! see JSON. Responses stay JSON: client-go's `Accept` header allows it.
//!
//! The JSON must match what Go would write, field by field, or
//! admission would check something other than what the client meant:
//!
//! - 64-bit integers stay JSON numbers. (Protobuf's own JSON mapping
//!   writes them as strings, which `as_i64()` reads as "not set".)
//! - Go writes every non-pointer field to protobuf, set or not, so
//!   `""`, `0` and `false` are on the wire for fields the client never
//!   touched. `proto/go-fields.json` (from `tools/k8s-proto-gen`) says
//!   for each field whether Go treats it as a pointer, `omitempty` or
//!   inline, and the converter applies Go's encoding/json rules.
//! - `Quantity`, `IntOrString`, `Time` and `FieldsV1` have their own
//!   JSON forms.
//! - A field the vendored `.proto` files don't know fails the request.
//!   Dropping it, as protobuf decoders do by default, would let a
//!   request through that admission rejects when sent as JSON.

use crate::k8s_status;
use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::http::{header, HeaderMap, HeaderValue, Method};
use axum::middleware::Next;
use axum::response::Response;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, ReflectMessage, Value,
};
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::OnceLock;

pub const MEDIA_TYPE: &str = "application/vnd.kubernetes.protobuf";

/// The 4-byte prefix of every Kubernetes protobuf body.
const MAGIC: &[u8] = b"k8s\0";

/// Kubernetes' own default limit for a request body is 3 MiB.
const MAX_BODY_BYTES: usize = 3 * 1024 * 1024;

const DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/k8s-descriptors.bin"));
const GO_FIELDS: &str = include_str!("../proto/go-fields.json");

const UNKNOWN: &str = "k8s.io.apimachinery.pkg.runtime.Unknown";
const TIME: &str = "k8s.io.apimachinery.pkg.apis.meta.v1.Time";
const FIELDS_V1: &str = "k8s.io.apimachinery.pkg.apis.meta.v1.FieldsV1";
const QUANTITY: &str = "k8s.io.apimachinery.pkg.api.resource.Quantity";
const INT_OR_STRING: &str = "k8s.io.apimachinery.pkg.util.intstr.IntOrString";

/// How Go's encoding/json writes one field (an entry of
/// `proto/go-fields.json`).
#[derive(Debug, Deserialize)]
struct GoField {
    json: String,
    #[serde(default)]
    omitempty: bool,
    #[serde(default)]
    pointer: bool,
    #[serde(default)]
    inline: bool,
}

struct Schema {
    pool: DescriptorPool,
    /// Message full name -> field number -> Go JSON encoding.
    go_fields: HashMap<String, HashMap<u32, GoField>>,
}

fn schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA.get_or_init(|| Schema {
        // Both are compiled in, and the tests below load them, so a
        // failure here is a build problem, not a runtime one.
        pool: DescriptorPool::decode(DESCRIPTORS).expect("embedded descriptors"),
        go_fields: serde_json::from_str(GO_FIELDS).expect("embedded go-fields.json"),
    })
}

/// The `apiVersion`/`kind` a protobuf body says it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeMeta {
    pub api_version: String,
    pub kind: String,
}

/// The protobuf message for each type kube-shim accepts as protobuf.
fn message_name(type_meta: &TypeMeta) -> Option<&'static str> {
    match (type_meta.api_version.as_str(), type_meta.kind.as_str()) {
        ("v1", "Secret") => Some("k8s.io.api.core.v1.Secret"),
        ("batch/v1", "CronJob") => Some("k8s.io.api.batch.v1.CronJob"),
        ("batch/v1", "Job") => Some("k8s.io.api.batch.v1.Job"),
        _ => None,
    }
}

/// Decodes a Kubernetes protobuf body (`k8s\0` + `runtime.Unknown`)
/// into the JSON object Go would write for it, `apiVersion` and `kind`
/// included.
pub fn decode(body: &[u8]) -> Result<(TypeMeta, JsonValue), String> {
    let schema = schema();
    let Some(envelope) = body.strip_prefix(MAGIC) else {
        return Err("missing the k8s\\0 protobuf prefix".to_string());
    };
    let unknown_desc = schema
        .pool
        .get_message_by_name(UNKNOWN)
        .ok_or("runtime.Unknown missing from descriptors")?;
    let unknown = DynamicMessage::decode(unknown_desc, envelope).map_err(|e| e.to_string())?;
    reject_unknown_fields(&unknown, "")?;

    let type_meta_msg = message_field(&unknown, "typeMeta");
    reject_unknown_fields(&type_meta_msg, "typeMeta")?;
    let type_meta = TypeMeta {
        api_version: string_field(&type_meta_msg, "apiVersion"),
        kind: string_field(&type_meta_msg, "kind"),
    };
    let content_encoding = string_field(&unknown, "contentEncoding");
    if !content_encoding.is_empty() {
        return Err(format!("unsupported contentEncoding {content_encoding:?}"));
    }
    let content_type = string_field(&unknown, "contentType");
    if !content_type.is_empty() && content_type != MEDIA_TYPE {
        return Err(format!("unsupported contentType {content_type:?}"));
    }
    let name = message_name(&type_meta).ok_or_else(|| {
        format!(
            "kube-shim doesn't accept {}/{} as protobuf",
            type_meta.api_version, type_meta.kind
        )
    })?;
    let desc = schema
        .pool
        .get_message_by_name(name)
        .ok_or_else(|| format!("{name} missing from descriptors"))?;
    let raw = match unknown.get_field_by_name("raw").as_deref() {
        Some(Value::Bytes(raw)) => raw.clone(),
        _ => Default::default(),
    };
    let object = DynamicMessage::decode(desc, raw).map_err(|e| e.to_string())?;

    let mut json = Map::new();
    json.insert("apiVersion".into(), type_meta.api_version.clone().into());
    json.insert("kind".into(), type_meta.kind.clone().into());
    write_fields(&object, &mut json, "")?;
    Ok((type_meta, JsonValue::Object(json)))
}

fn message_field(msg: &DynamicMessage, name: &str) -> DynamicMessage {
    match msg.get_field_by_name(name).as_deref() {
        Some(Value::Message(m)) => m.clone(),
        _ => unreachable!("{name} is a message field"),
    }
}

fn string_field(msg: &DynamicMessage, name: &str) -> String {
    match msg.get_field_by_name(name).as_deref() {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn reject_unknown_fields(msg: &DynamicMessage, path: &str) -> Result<(), String> {
    match msg.unknown_fields().next() {
        None => Ok(()),
        Some(field) => Err(format!(
            "{}: field number {} is unknown to kube-shim's {}",
            display_path(path),
            field.number(),
            msg.descriptor().full_name()
        )),
    }
}

fn display_path(path: &str) -> &str {
    if path.is_empty() {
        "<object>"
    } else {
        path
    }
}

fn join(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

/// Writes the JSON fields of `msg` into `out`, following Go's
/// encoding/json rules for each field (see the module docs).
fn write_fields(
    msg: &DynamicMessage,
    out: &mut Map<String, JsonValue>,
    path: &str,
) -> Result<(), String> {
    reject_unknown_fields(msg, path)?;
    let desc = msg.descriptor();
    let go_fields = schema().go_fields.get(desc.full_name()).ok_or_else(|| {
        format!(
            "{}: {} isn't supported",
            display_path(path),
            desc.full_name()
        )
    })?;
    for field in desc.fields() {
        let go = go_fields.get(&field.number()).ok_or_else(|| {
            format!(
                "{}.{} missing from go-fields.json",
                desc.full_name(),
                field.name()
            )
        })?;
        let present = msg.has_field(&field);
        if go.inline {
            // An embedded Go struct, e.g. Volume.VolumeSource: its
            // fields are written as if they were the parent's own.
            let inner = match msg.get_field(&field).as_ref() {
                Value::Message(m) => m.clone(),
                _ => {
                    return Err(format!(
                        "{}.{}: inline non-message",
                        desc.full_name(),
                        field.name()
                    ))
                }
            };
            write_fields(&inner, out, path)?;
            continue;
        }
        let field_path = join(path, &go.json);
        let value = if present {
            let value = msg.get_field(&field);
            if !go.pointer && go.omitempty && is_empty(&value) {
                continue;
            }
            field_to_json(&field, &value, &field_path)?
        } else if go.pointer || field.is_list() || field.is_map() || field.kind() == Kind::Bytes {
            // nil in Go.
            if go.omitempty {
                continue;
            }
            JsonValue::Null
        } else if let Kind::Message(m) = field.kind() {
            // A non-pointer struct: Go writes it even when it's zero,
            // whatever omitempty says.
            message_to_json(&DynamicMessage::new(m), &field_path)?
        } else {
            if go.omitempty {
                continue;
            }
            scalar_to_json(&field.default_value(), &field_path)?
        };
        out.insert(go.json.clone(), value);
    }
    Ok(())
}

/// Whether Go's `omitempty` drops this value of a non-pointer field.
/// Never true for a struct (message): encoding/json doesn't omit those.
fn is_empty(value: &Value) -> bool {
    match value {
        Value::Bool(b) => !b,
        Value::I32(n) => *n == 0,
        Value::I64(n) => *n == 0,
        Value::U32(n) => *n == 0,
        Value::U64(n) => *n == 0,
        Value::F32(n) => *n == 0.0,
        Value::F64(n) => *n == 0.0,
        Value::String(s) => s.is_empty(),
        Value::Bytes(b) => b.is_empty(),
        Value::List(l) => l.is_empty(),
        Value::Map(m) => m.is_empty(),
        Value::EnumNumber(_) | Value::Message(_) => false,
    }
}

fn field_to_json(field: &FieldDescriptor, value: &Value, path: &str) -> Result<JsonValue, String> {
    match value {
        Value::List(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| value_to_json(item, &format!("{path}[{i}]")))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        Value::Map(entries) => {
            let mut out = Map::new();
            for (key, item) in entries {
                let MapKey::String(key) = key else {
                    return Err(format!(
                        "{path}: non-string map key in {}",
                        field.full_name()
                    ));
                };
                out.insert(key.clone(), value_to_json(item, &format!("{path}.{key}"))?);
            }
            Ok(JsonValue::Object(out))
        }
        other => value_to_json(other, path),
    }
}

fn value_to_json(value: &Value, path: &str) -> Result<JsonValue, String> {
    match value {
        Value::Message(m) => message_to_json(m, path),
        other => scalar_to_json(other, path),
    }
}

fn scalar_to_json(value: &Value, path: &str) -> Result<JsonValue, String> {
    Ok(match value {
        Value::Bool(b) => (*b).into(),
        Value::I32(n) => (*n).into(),
        Value::I64(n) => (*n).into(),
        Value::U32(n) => (*n).into(),
        Value::U64(n) => (*n).into(),
        Value::F32(n) => serde_json::Number::from_f64(f64::from(*n))
            .map(JsonValue::Number)
            .ok_or_else(|| format!("{path}: not a finite number"))?,
        Value::F64(n) => serde_json::Number::from_f64(*n)
            .map(JsonValue::Number)
            .ok_or_else(|| format!("{path}: not a finite number"))?,
        Value::String(s) => s.clone().into(),
        // Go writes []byte as standard base64, with padding.
        Value::Bytes(b) => BASE64.encode(b).into(),
        other => return Err(format!("{path}: unsupported protobuf value {other:?}")),
    })
}

fn message_to_json(msg: &DynamicMessage, path: &str) -> Result<JsonValue, String> {
    let name = msg.descriptor().full_name().to_string();
    if matches!(name.as_str(), TIME | FIELDS_V1 | QUANTITY | INT_OR_STRING) {
        reject_unknown_fields(msg, path)?;
    }
    match name.as_str() {
        // metav1.Time: zero is null; otherwise RFC 3339 in UTC, whole
        // seconds (Go drops the nanoseconds in JSON).
        TIME => {
            let seconds = int_field(msg, "seconds");
            let nanos = int_field(msg, "nanos");
            if seconds == 0 && nanos == 0 {
                return Ok(JsonValue::Null);
            }
            chrono::DateTime::from_timestamp(seconds, 0)
                .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true).into())
                .ok_or_else(|| format!("{path}: timestamp out of range"))
        }
        // resource.Quantity: Go writes the canonical string to protobuf
        // already; a zero Quantity is "0".
        QUANTITY => {
            let s = string_field(msg, "string");
            Ok(if s.is_empty() { "0".into() } else { s.into() })
        }
        // intstr.IntOrString: a number or a string, depending on `type`.
        INT_OR_STRING => match int_field(msg, "type") {
            0 => Ok(int_field(msg, "intVal").into()),
            1 => Ok(string_field(msg, "strVal").into()),
            other => Err(format!("{path}: unknown IntOrString type {other}")),
        },
        // metav1.FieldsV1: raw JSON, as in managedFields.
        FIELDS_V1 => match msg.get_field_by_name("Raw").as_deref() {
            Some(Value::Bytes(raw)) if !raw.is_empty() => serde_json::from_slice(raw)
                .map_err(|e| format!("{path}: invalid FieldsV1 JSON: {e}")),
            _ => Ok(JsonValue::Null),
        },
        _ => {
            let mut out = Map::new();
            write_fields(msg, &mut out, path)?;
            Ok(JsonValue::Object(out))
        }
    }
}

fn int_field(msg: &DynamicMessage, name: &str) -> i64 {
    match msg.get_field_by_name(name).as_deref() {
        Some(Value::I32(n)) => i64::from(*n),
        Some(Value::I64(n)) => *n,
        _ => 0,
    }
}

/// The media type of a request, without parameters such as `charset`.
pub fn media_type(headers: &HeaderMap) -> &str {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(str::trim)
        .unwrap_or("")
}

fn is_json(media_type: &str) -> bool {
    media_type == "application/json" || media_type.ends_with("+json")
}

/// The object a protobuf `POST`/`PUT` to `path` must hold, or `None` if
/// kube-shim doesn't take protobuf there.
fn expected_type(method: &Method, path: &str) -> Option<TypeMeta> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let (api_version, kind) = match (method, segments.as_slice()) {
        (&Method::POST, ["api", "v1", "namespaces", _, "secrets"]) => ("v1", "Secret"),
        (&Method::POST, ["apis", "batch", "v1", "namespaces", _, "cronjobs"])
        | (&Method::PUT, ["apis", "batch", "v1", "namespaces", _, "cronjobs", _]) => {
            ("batch/v1", "CronJob")
        }
        (&Method::POST, ["apis", "batch", "v1", "namespaces", _, "jobs"]) => ("batch/v1", "Job"),
        _ => return None,
    };
    Some(TypeMeta {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
    })
}

/// Middleware on the API routes: converts protobuf `POST`/`PUT` bodies
/// to JSON (see the module docs) and answers other unsupported request
/// bodies with a Kubernetes `Status`. `DELETE` bodies (client-go sends
/// `DeleteOptions` as protobuf) pass through: the handlers ignore them.
pub async fn protobuf_bodies(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    if method != Method::POST && method != Method::PUT {
        return next.run(request).await;
    }
    let media_type = media_type(request.headers()).to_string();
    if is_json(&media_type) {
        return next.run(request).await;
    }
    if media_type != MEDIA_TYPE {
        return k8s_status::unsupported_media_type(format!(
            "the body of the request was in an unknown format - accepted media types \
             include: application/json, {MEDIA_TYPE} (got {media_type:?})"
        ));
    }
    let Some(expected) = expected_type(&method, request.uri().path()) else {
        return k8s_status::unsupported_media_type(format!(
            "kube-shim accepts {MEDIA_TYPE} only for creating Secrets, CronJobs and Jobs \
             and for replacing CronJobs; send application/json here"
        ));
    };

    let (mut parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(e) => return k8s_status::bad_request(format!("reading request body: {e}")),
    };
    let (type_meta, json) = match decode(&bytes) {
        Ok(decoded) => decoded,
        Err(e) => return k8s_status::bad_request(format!("invalid protobuf body: {e}")),
    };
    if type_meta != expected {
        return k8s_status::bad_request(format!(
            "the body holds {}/{}, but this endpoint takes {}/{}",
            type_meta.api_version, type_meta.kind, expected.api_version, expected.kind
        ));
    }
    let json = match serde_json::to_vec(&json) {
        Ok(json) => json,
        Err(e) => return k8s_status::bad_request(e.to_string()),
    };
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    parts.headers.remove(header::CONTENT_LENGTH);
    next.run(Request::from_parts(parts, Body::from(json))).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every message the descriptors can reach from the three accepted
    /// types has an entry in go-fields.json, for every field, and the
    /// JSON names agree.
    #[test]
    fn test_go_fields_cover_descriptors() {
        let schema = schema();
        let mut todo = vec![
            "k8s.io.api.core.v1.Secret".to_string(),
            "k8s.io.api.batch.v1.CronJob".to_string(),
            "k8s.io.api.batch.v1.Job".to_string(),
        ];
        let mut seen = std::collections::HashSet::new();
        while let Some(name) = todo.pop() {
            if !seen.insert(name.clone())
                || matches!(name.as_str(), TIME | FIELDS_V1 | QUANTITY | INT_OR_STRING)
            {
                continue;
            }
            let desc = schema.pool.get_message_by_name(&name).unwrap();
            let go = schema
                .go_fields
                .get(&name)
                .unwrap_or_else(|| panic!("{name} missing from go-fields.json"));
            assert_eq!(go.len(), desc.fields().len(), "{name}");
            for field in desc.fields() {
                let g = go
                    .get(&field.number())
                    .unwrap_or_else(|| panic!("{name}.{}", field.name()));
                if !g.inline {
                    assert_eq!(g.json, field.name(), "{name}.{}", field.name());
                }
                if let Kind::Message(m) = field.kind() {
                    if !field.is_map() {
                        todo.push(m.full_name().to_string());
                    } else if let Kind::Message(v) = m.map_entry_value_field().kind() {
                        todo.push(v.full_name().to_string());
                    }
                }
            }
        }
    }

    #[test]
    fn test_expected_type() {
        let cronjob = Some(TypeMeta {
            api_version: "batch/v1".into(),
            kind: "CronJob".into(),
        });
        assert_eq!(
            expected_type(&Method::POST, "/apis/batch/v1/namespaces/default/cronjobs"),
            cronjob
        );
        assert_eq!(
            expected_type(&Method::PUT, "/apis/batch/v1/namespaces/default/cronjobs/x"),
            cronjob
        );
        assert_eq!(
            expected_type(&Method::PUT, "/apis/batch/v1/namespaces/default/cronjobs"),
            None
        );
        assert_eq!(
            expected_type(&Method::POST, "/api/v1/namespaces/ns/secrets")
                .unwrap()
                .kind,
            "Secret"
        );
        assert_eq!(
            expected_type(&Method::POST, "/apis/batch/v1/namespaces/ns/jobs")
                .unwrap()
                .kind,
            "Job"
        );
        assert_eq!(
            expected_type(
                &Method::POST,
                "/apis/cost.kube-shim.brawer.ch/v1/budget/topup"
            ),
            None
        );
    }

    #[test]
    fn test_rejects_missing_magic() {
        assert!(decode(b"{}").unwrap_err().contains("prefix"));
    }
}
