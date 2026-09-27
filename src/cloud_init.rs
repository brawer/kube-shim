//! Cloud-init (UpCloud calls it a server's `user_data`) generation for
//! worker VMs (Phase 9).
//!
//! A job's container spec (image, command, args, env) is user-supplied --
//! it comes straight from a CronJob's pod template, submitted over the
//! authenticated API by whoever holds a bearer token. Interpolating any
//! of it directly into the shell script UpCloud runs as root on first
//! boot would be a textbook command-injection vector (an `image` of
//! `"x; rm -rf / #"` or similar). Instead, every job-supplied value is
//! base64-encoded here and substituted into `bootstrap/cloud-init-
//! template.sh` as an opaque blob; the template decodes each one with
//! `base64 -d` and only ever uses the result as a literal string/argv
//! entry (`podman run "$IMAGE" "${COMMAND[@]}" "${ARGS[@]}"`), never by
//! concatenating it into a shell command string. Base64's own alphabet
//! (`A-Za-z0-9+/=`) contains no shell metacharacters, so embedding it
//! inside the template's single-quoted `'...'` literals is safe by
//! construction, not just by convention.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::Value as JsonValue;

const TEMPLATE: &str = include_str!("../bootstrap/cloud-init-template.sh");

/// Builds the real `user_data` script for a job's worker VM, from
/// `spec.template.spec.containers[0]` -- this project only ever reads the
/// *first* container of a pod template, matching every other pod-template
/// reader in this codebase (e.g. `reconcile::job`'s own resource-request
/// extraction); multi-container pods aren't supported.
///
/// Returns an error (rather than silently generating a script that can
/// only fail once it actually runs) if there's no image to run --
/// `VMPending`'s caller treats this as a normal failed-attempt retry
/// (`record_failed_attempt`), same as any other real-call failure, since
/// there's no admission-time check yet rejecting a container with no
/// image (see `src/admission.rs`'s existing checks, neither of which
/// covers this).
pub fn generate(spec: &JsonValue) -> Result<String, String> {
    let container = spec
        .pointer("/template/spec/containers/0")
        .ok_or_else(|| "job spec has no template.spec.containers[0]".to_string())?;

    let image = container
        .get("image")
        .and_then(JsonValue::as_str)
        .filter(|image| !image.is_empty())
        .ok_or_else(|| "containers[0].image is missing, empty, or not a string".to_string())?;

    let command = string_array(container, "command").join("\n");
    let args = string_array(container, "args").join("\n");
    let env = env_lines(container).join("\n");

    Ok(TEMPLATE
        .replace("__KUBESHIM_IMAGE_B64__", &b64(image))
        .replace("__KUBESHIM_COMMAND_B64__", &b64(&command))
        .replace("__KUBESHIM_ARGS_B64__", &b64(&args))
        .replace("__KUBESHIM_ENV_B64__", &b64(&env)))
}

fn string_array(container: &JsonValue, field: &str) -> Vec<String> {
    container
        .get(field)
        .and_then(JsonValue::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(JsonValue::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// `containers[0].env[]`, Kubernetes' `{name, value}` shape -- plain
/// values only. An entry using `valueFrom` (e.g. `secretKeyRef`, pulling
/// from this project's own real `Secret` support) is silently skipped
/// rather than failing the whole job over it; wiring that up is future
/// work, not scoped to Phase 9 (see docs/IMPLEMENTATION_PLAN.md, which
/// never mentions it).
fn env_lines(container: &JsonValue) -> Vec<String> {
    container
        .get("env")
        .and_then(JsonValue::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let name = entry.get("name")?.as_str()?;
                    let value = entry.get("value")?.as_str()?;
                    Some(format!("{name}={value}"))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn b64(s: &str) -> String {
    BASE64.encode(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decode_between(script: &str, after: &str) -> String {
        // Pulls the single-quoted base64 blob immediately following
        // `after` (one of the script's own `echo '<blob>' | base64 -d`
        // lines) back out, so tests can assert on the *decoded* value
        // rather than fragile-to-encoding-details base64 text.
        let start = script.find(after).unwrap() + after.len();
        let rest = &script[start..];
        let quote_start = rest.find('\'').unwrap() + 1;
        let quote_end = rest[quote_start..].find('\'').unwrap() + quote_start;
        let b64_blob = &rest[quote_start..quote_end];
        String::from_utf8(BASE64.decode(b64_blob).unwrap()).unwrap()
    }

    fn container_spec(container: JsonValue) -> JsonValue {
        json!({"template": {"spec": {"containers": [container]}}})
    }

    #[test]
    fn test_generates_script_with_image_only() {
        let spec = container_spec(json!({"image": "busybox:latest"}));
        let script = generate(&spec).unwrap();

        // The template's own comments mention the placeholder *names*
        // (e.g. "__KUBESHIM_IMAGE_B64__ placeholders"), so assert the
        // exact tokens are gone, not a bare "__KUBESHIM_" substring.
        for placeholder in [
            "__KUBESHIM_IMAGE_B64__",
            "__KUBESHIM_COMMAND_B64__",
            "__KUBESHIM_ARGS_B64__",
            "__KUBESHIM_ENV_B64__",
        ] {
            assert!(
                !script.contains(placeholder),
                "{placeholder} must be substituted"
            );
        }
        assert_eq!(decode_between(&script, "IMAGE=$(echo "), "busybox:latest");
        assert_eq!(decode_between(&script, "COMMAND < <(echo "), "");
        assert_eq!(decode_between(&script, "ARGS < <(echo "), "");
        assert_eq!(decode_between(&script, "ENV_LINES < <(echo "), "");
    }

    #[test]
    fn test_encodes_command_args_env() {
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "command": ["sh", "-c"],
            "args": ["echo hello > /scratch/out.txt"],
            "env": [
                {"name": "FOO", "value": "bar"},
                {"name": "BAZ", "value": "qux"}
            ]
        }));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "IMAGE=$(echo "), "busybox:latest");
        assert_eq!(decode_between(&script, "COMMAND < <(echo "), "sh\n-c");
        assert_eq!(
            decode_between(&script, "ARGS < <(echo "),
            "echo hello > /scratch/out.txt"
        );
        assert_eq!(
            decode_between(&script, "ENV_LINES < <(echo "),
            "FOO=bar\nBAZ=qux"
        );
    }

    #[test]
    fn test_env_entry_with_value_from_is_skipped_not_fatal() {
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "env": [
                {"name": "FROM_SECRET", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}},
                {"name": "PLAIN", "value": "ok"}
            ]
        }));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "ENV_LINES < <(echo "), "PLAIN=ok");
    }

    #[test]
    fn test_missing_image_is_an_error() {
        let spec = container_spec(json!({}));
        assert!(generate(&spec).is_err());
    }

    #[test]
    fn test_empty_image_is_an_error() {
        let spec = container_spec(json!({"image": ""}));
        assert!(generate(&spec).is_err());
    }

    #[test]
    fn test_no_containers_is_an_error() {
        let spec = json!({"template": {"spec": {"containers": []}}});
        assert!(generate(&spec).is_err());
    }

    #[test]
    fn test_image_with_shell_metacharacters_cannot_break_out_of_the_script() {
        // The whole point of base64-encoding job-supplied values: this
        // would be a command injection if ever interpolated as raw text.
        let spec = container_spec(json!({"image": "x'; rm -rf / #"}));
        let script = generate(&spec).unwrap();

        assert!(!script.contains("rm -rf /"));
        assert_eq!(decode_between(&script, "IMAGE=$(echo "), "x'; rm -rf / #");
    }
}
