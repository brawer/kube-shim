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

//!
//! **Only the image, resource limits and run options (`securityContext`,
//! scratch mount path; see `src/run_options.rs`) travel in `user_data`.** The
//! container's environment (which may hold `Secret` values) and its
//! `command`/`args` (which may embed them via `$(VAR)` expansion) are
//! delivered later over SSH instead (`ContainerConfig`, uploaded by
//! `reconcile::job`'s `ContainerConfiguring` state into root-only files
//! under `CONFIG_DIR`), so they are never readable through UpCloud's API
//! by anyone who can read the server's `user_data` (issue #84). The
//! template waits for `READY_PATH` before it starts the container.

use crate::{container_env, run_options, volumes, workload};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::Value as JsonValue;

const TEMPLATE: &str = include_str!("../bootstrap/cloud-init-template.sh");

/// Where the worker keeps the SSH-delivered container config. `/run` is
/// a tmpfs, so nothing here ever reaches the boot disk.
/// `bootstrap/cloud-init-template.sh` hardcodes the same paths.
pub const CONFIG_DIR: &str = "/run/kube-shim";

/// The fixed remote commands `reconcile::job` runs (with the file's
/// contents on stdin) to deliver each file: written root-only (`umask
/// 077`) and renamed into place, so the template never reads a
/// half-written file. `READY_COMMAND` goes last, once all four exist.
pub const UPLOAD_ENV_COMMAND: &str = "umask 077 && mkdir -p /run/kube-shim && \
     cat > /run/kube-shim/env.part && mv -f /run/kube-shim/env.part /run/kube-shim/env";
pub const UPLOAD_COMMAND_COMMAND: &str = "umask 077 && mkdir -p /run/kube-shim && \
     cat > /run/kube-shim/command.part && mv -f /run/kube-shim/command.part /run/kube-shim/command";
pub const UPLOAD_ARGS_COMMAND: &str = "umask 077 && mkdir -p /run/kube-shim && \
     cat > /run/kube-shim/args.part && mv -f /run/kube-shim/args.part /run/kube-shim/args";
pub const UPLOAD_SECRET_ENV_COMMAND: &str = "umask 077 && mkdir -p /run/kube-shim && \
     cat > /run/kube-shim/secret-env.part && \
     mv -f /run/kube-shim/secret-env.part /run/kube-shim/secret-env";
pub const READY_COMMAND: &str = "umask 077 && cat > /run/kube-shim/ready";

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
    let container = first_container(spec)?;

    let image = container
        .get("image")
        .and_then(JsonValue::as_str)
        .filter(|image| !image.is_empty())
        .ok_or_else(|| "containers[0].image is missing, empty, or not a string".to_string())?;

    let cpu_limit = cpu_limit_flag_value(container);
    let memory_limit = memory_limit_flag_value(container);

    // Rejected at admission already; a job stored before that check
    // existed fails loudly here rather than running without, say, the
    // fsGroup it needs to write its scratch space.
    let options = run_options::parse(spec)
        .map_err(|e| format!("template.spec.{} is invalid: {:?}", e.field, e.value))?;
    let fs_group = options.fs_group.map(|g| g.to_string()).unwrap_or_default();
    let user = options.user.unwrap_or_default();
    let read_only_root = if options.read_only_root_filesystem {
        "true"
    } else {
        ""
    };
    let run_as_non_root = if options.run_as_non_root { "true" } else { "" };
    let no_new_privileges = if options.no_new_privileges {
        "true"
    } else {
        ""
    };
    // Tells the worker whether to wait for a volume device at all
    // (issue #135); `handle_volume_pending` creates one exactly when
    // this finds it.
    let volume_expected = if crate::reconcile::job::find_ephemeral_volume(spec).is_some() {
        "true"
    } else {
        ""
    };

    Ok(TEMPLATE
        .replace("__KUBESHIM_IMAGE_B64__", &b64(image))
        .replace("__KUBESHIM_CPU_LIMIT_B64__", &b64(&cpu_limit))
        .replace("__KUBESHIM_MEMORY_LIMIT_B64__", &b64(&memory_limit))
        .replace("__KUBESHIM_FS_GROUP_B64__", &b64(&fs_group))
        .replace("__KUBESHIM_RUN_AS_USER_B64__", &b64(&user))
        .replace("__KUBESHIM_READ_ONLY_ROOT_B64__", &b64(read_only_root))
        .replace(
            "__KUBESHIM_NO_NEW_PRIVILEGES_B64__",
            &b64(no_new_privileges),
        )
        .replace("__KUBESHIM_RUN_AS_NON_ROOT_B64__", &b64(run_as_non_root))
        .replace(
            "__KUBESHIM_SCRATCH_VOLUME_B64__",
            &b64(&options.scratch_volume),
        )
        .replace("__KUBESHIM_VOLUME_EXPECTED_B64__", &b64(volume_expected)))
}

/// `spec.template.spec.containers[0]`.
pub fn first_container(spec: &JsonValue) -> Result<&JsonValue, String> {
    spec.pointer("/template/spec/containers/0")
        .ok_or_else(|| "job spec has no template.spec.containers[0]".to_string())
}

/// The files the worker's container is started from, delivered over SSH
/// (see the module docs).
#[derive(Debug, PartialEq, Eq)]
pub struct ContainerConfig {
    /// `podman run --env-file` format: one `NAME=VALUE` per line.
    pub env_file: Vec<u8>,
    /// Variables `--env-file` can't hold (a line break, or longer than
    /// one of its lines may be -- a PEM key, say), as NUL-terminated
    /// `NAME`, `VALUE` pairs. The worker turns each into a podman secret
    /// (`podman secret create`, value on stdin) and passes it with
    /// `--secret ...,type=env,target=NAME`, which sets the variable to
    /// the exact bytes.
    pub secret_env: Vec<u8>,
    /// `command`/`args` after Kubernetes' `$(VAR)` expansion, each entry
    /// NUL-terminated (read back with `mapfile -d ''`), so an entry can
    /// hold any text, newlines included.
    pub command: Vec<u8>,
    pub args: Vec<u8>,
}

/// podman's `--env-file` parser reads one variable per line, takes
/// everything after the first `=` literally (no quoting), skips lines
/// starting with `#`, strips leading whitespace from the name, and a
/// line longer than 64KiB fails to parse. A value it can't hold goes to
/// `secret_env` instead; a name it can't hold is refused, loudly.
const MAX_ENV_FILE_LINE: usize = 60 * 1024;

/// podman's own limit on a secret's size.
const MAX_SECRET_ENV_VALUE: usize = 512 * 1024;

pub fn container_config(
    container: &JsonValue,
    env: &[(String, String)],
) -> Result<ContainerConfig, String> {
    let mut env_file = Vec::new();
    let mut secret_env = Vec::new();
    for (name, value) in env {
        if name.starts_with([' ', '#']) {
            return Err(format!(
                "environment variable name {name:?} can't start with a space or '#' \
                 (not representable in podman's --env-file)"
            ));
        }
        let line = format!("{name}={value}\n");
        if !value.contains(['\n', '\r']) && line.len() <= MAX_ENV_FILE_LINE {
            env_file.extend_from_slice(line.as_bytes());
            continue;
        }
        if name.contains(',') {
            // `--secret` options are comma-separated.
            return Err(format!(
                "environment variable {name:?} has a multi-line or very long value, which \
                 is only supported for names without ','"
            ));
        }
        if value.len() > MAX_SECRET_ENV_VALUE {
            return Err(format!(
                "environment variable {name} is longer than {MAX_SECRET_ENV_VALUE} bytes, \
                 podman's limit for a secret"
            ));
        }
        secret_env.extend_from_slice(name.as_bytes());
        secret_env.push(0);
        secret_env.extend_from_slice(value.as_bytes());
        secret_env.push(0);
    }

    let nul_terminated = |field: &str| -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for entry in string_array(container, field) {
            let expanded = container_env::expand(&entry, env);
            if expanded.contains('\0') {
                return Err(format!("containers[0].{field} contains a NUL byte"));
            }
            out.extend_from_slice(expanded.as_bytes());
            out.push(0);
        }
        Ok(out)
    };

    Ok(ContainerConfig {
        env_file,
        secret_env,
        command: nul_terminated("command")?,
        args: nul_terminated("args")?,
    })
}

fn resource_limit_quantity<'a>(container: &'a JsonValue, key: &str) -> Option<&'a str> {
    container.pointer("/resources/limits")?.get(key)?.as_str()
}

/// `resources.limits.cpu`, as the decimal core count `podman run
/// --cpus` expects (e.g. `"0.5"`, `"2"`) -- *not* rounded up to a whole
/// core the way `handle_vm_pending`'s own VM sizing needs to be (UpCloud
/// has no fractional-core plans; podman's `--cpus` has no such
/// restriction, so there's no reason to lose precision here). Empty
/// when unset, which the template then omits the flag entirely for --
/// matching real Kubernetes semantics, where no limit means
/// unconstrained, not "capped at the request" or "capped at whatever VM
/// plan got picked."
fn cpu_limit_flag_value(container: &JsonValue) -> String {
    let Some(quantity) = resource_limit_quantity(container, "cpu") else {
        return String::new();
    };
    let Ok(millicores) = workload::parse_cpu_millicores(quantity) else {
        return String::new();
    };
    if millicores.is_multiple_of(1000) {
        (millicores / 1000).to_string()
    } else {
        // Exact for any u32 millicore value -- three decimal places is
        // this project's own precision floor (parse_cpu_millicores
        // never produces a finer-grained value), so this never needs
        // real rounding, just trimming trailing zeros for readability.
        format!("{:.3}", millicores as f64 / 1000.0)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

/// `resources.limits.memory`, as the `podman run --memory` flag expects
/// -- a byte count with podman's own `b`/`k`/`m`/`g` suffix convention,
/// *not* Kubernetes' `Ki`/`Mi`/`Gi`. `m` here is the same 1024-based
/// megabyte `volumes::parse_storage_quantity_mb` already uses, so this
/// is a direct, lossless handoff, not a second unit conversion. Empty
/// when unset -- see `cpu_limit_flag_value`'s own docs on why that's
/// the correct default, not "cap at the request."
fn memory_limit_flag_value(container: &JsonValue) -> String {
    let Some(quantity) = resource_limit_quantity(container, "memory") else {
        return String::new();
    };
    let Ok(mb) = volumes::parse_storage_quantity_mb(quantity) else {
        return String::new();
    };
    format!("{mb}m")
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
            "__KUBESHIM_CPU_LIMIT_B64__",
            "__KUBESHIM_MEMORY_LIMIT_B64__",
            "__KUBESHIM_FS_GROUP_B64__",
            "__KUBESHIM_RUN_AS_USER_B64__",
            "__KUBESHIM_READ_ONLY_ROOT_B64__",
            "__KUBESHIM_RUN_AS_NON_ROOT_B64__",
            "__KUBESHIM_NO_NEW_PRIVILEGES_B64__",
            "__KUBESHIM_SCRATCH_VOLUME_B64__",
            "__KUBESHIM_VOLUME_EXPECTED_B64__",
        ] {
            assert!(
                !script.contains(placeholder),
                "{placeholder} must be substituted"
            );
        }
        assert_eq!(decode_between(&script, "IMAGE=$(echo "), "busybox:latest");
        assert_eq!(decode_between(&script, "FS_GROUP=$(echo "), "");
        assert_eq!(decode_between(&script, "RUN_AS_USER=$(echo "), "");
        assert_eq!(decode_between(&script, "READ_ONLY_ROOT=$(echo "), "");
        assert_eq!(decode_between(&script, "NO_NEW_PRIVILEGES=$(echo "), "");
        assert_eq!(decode_between(&script, "RUN_AS_NON_ROOT=$(echo "), "");
        assert_eq!(
            decode_between(&script, "SCRATCH_VOLUME=$(echo "),
            "/scratch:/scratch:idmap"
        );
        assert_eq!(decode_between(&script, "VOLUME_EXPECTED=$(echo "), "");
    }

    #[test]
    fn test_only_jobs_with_a_volume_wait_for_one() {
        // Issue #135: without a volume, the worker must not poll for a
        // device that never comes.
        let without = generate(&container_spec(json!({"image": "x"}))).unwrap();
        assert_eq!(decode_between(&without, "VOLUME_EXPECTED=$(echo "), "");
        let with = generate(&json!({"template": {"spec": {
            "containers": [{"image": "x"}],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
        }}}))
        .unwrap();
        assert_eq!(decode_between(&with, "VOLUME_EXPECTED=$(echo "), "true");
        assert!(with.contains(
            "if [ \"$VOLUME_EXPECTED\" = true ]; then\n    echo \"Waiting for volume...\""
        ));
    }

    #[test]
    fn test_run_options_reach_the_script() {
        // Issue #86: the osmdiffs CronJob, plus runAsUser and a mountPath.
        let spec = json!({"template": {"spec": {
            "securityContext": {"fsGroup": 1000, "runAsUser": 1000, "runAsNonRoot": true},
            "containers": [{
                "image": "x",
                "securityContext": {"readOnlyRootFilesystem": true, "allowPrivilegeEscalation": false},
                "volumeMounts": [{"name": "scratch", "mountPath": "/workdir"}]
            }],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
        }}});
        let script = generate(&spec).unwrap();
        assert_eq!(decode_between(&script, "FS_GROUP=$(echo "), "1000");
        assert_eq!(decode_between(&script, "RUN_AS_USER=$(echo "), "1000");
        assert_eq!(decode_between(&script, "READ_ONLY_ROOT=$(echo "), "true");
        assert_eq!(decode_between(&script, "NO_NEW_PRIVILEGES=$(echo "), "true");
        assert_eq!(decode_between(&script, "RUN_AS_NON_ROOT=$(echo "), "true");
        assert_eq!(
            decode_between(&script, "SCRATCH_VOLUME=$(echo "),
            "/scratch:/workdir:idmap"
        );
        assert!(script.contains("chgrp \"$FS_GROUP\" /scratch"));
        assert!(script.contains("chmod 2775 /scratch"));
        // Always: a user namespace, with subordinate ids to draw from.
        assert!(script.contains("SECURITY_ARGS=(--userns auto:size=2147483648)"));
        assert!(script.contains("containers:2147483647:2147483648\" >> /etc/subuid"));
    }

    #[test]
    fn test_invalid_run_options_are_an_error() {
        let spec = json!({"template": {"spec": {
            "securityContext": {"fsGroup": "1000; reboot"},
            "containers": [{"image": "x"}]
        }}});
        assert!(generate(&spec).is_err());
    }

    #[test]
    fn test_container_gets_the_workers_ipv4_resolvers() {
        // Issue #130: explicit `--dns`, from systemd-resolved's upstream
        // list, without loopback or IPv6 servers.
        let script = generate(&container_spec(json!({"image": "busybox:latest"}))).unwrap();
        assert!(script.contains("UPSTREAM_RESOLV_CONF=/run/systemd/resolve/resolv.conf"));
        assert!(script.contains("127.* | *:*) continue ;;"));
        assert!(script.contains("DNS_ARGS+=(--dns \"$server\")"));
        let run_line = script
            .lines()
            .find(|l| l.starts_with("CONTAINER_ID=$(podman run"))
            .unwrap();
        assert!(run_line.contains("\"${DNS_ARGS[@]}\""));
        assert!(run_line.ends_with("2>> \"$STATUS_FILE\")"));
    }

    #[test]
    fn test_user_data_never_contains_env_command_or_args() {
        // Issue #84: these may hold Secret values, so they travel over
        // SSH (ContainerConfig), never in user_data.
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "command": ["sh", "-c"],
            "args": ["echo very-secret-arg"],
            "env": [{"name": "FOO", "value": "very-secret-value"}]
        }));
        let script = generate(&spec).unwrap();
        for needle in ["very-secret-arg", "very-secret-value", "FOO"] {
            assert!(!script.contains(needle), "{needle} leaked into user_data");
        }
        assert!(!script.contains(&BASE64.encode("FOO=very-secret-value")));
        assert!(!script.contains(&BASE64.encode("echo very-secret-arg")));
        // The template reads them from the SSH-delivered files instead.
        assert!(script.contains("CONFIG_DIR=/run/kube-shim"));
        assert!(script.contains("--env-file \"$CONFIG_DIR/env\""));
    }

    fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_container_config_env_file_and_nul_terminated_argv() {
        let container = json!({
            "image": "busybox:latest",
            "command": ["sh", "-c"],
            "args": ["echo hello\nworld > /scratch/out.txt", ""]
        });
        let env = pairs(&[("FOO", "bar=baz"), ("EMPTY", ""), ("Q", "\"quoted\" #x")]);
        let config = container_config(&container, &env).unwrap();

        assert_eq!(
            config.env_file,
            b"FOO=bar=baz\nEMPTY=\nQ=\"quoted\" #x\n".to_vec()
        );
        assert_eq!(config.command, b"sh\0-c\0".to_vec());
        assert_eq!(
            config.args,
            b"echo hello\nworld > /scratch/out.txt\0\0".to_vec()
        );
    }

    #[test]
    fn test_container_config_expands_vars_in_command_and_args() {
        // The osmdiffs args from issue #84.
        let container = json!({
            "image": "x",
            "args": ["run", "--workdir", "/workdir", "--run_id", "$(RUN_ID)", "$$(RUN_ID)"]
        });
        let env = pairs(&[("RUN_ID", "osmdiffs-1762410000")]);
        let config = container_config(&container, &env).unwrap();
        assert_eq!(
            config.args,
            b"run\0--workdir\0/workdir\0--run_id\0osmdiffs-1762410000\0$(RUN_ID)\0".to_vec()
        );
        assert!(config.command.is_empty());
    }

    #[test]
    fn test_container_config_multi_line_and_long_values_go_to_secret_env() {
        let container = json!({"image": "x"});
        let pem = "-----BEGIN KEY-----\nabc def\r\n-----END KEY-----\n";
        let long = "x".repeat(MAX_ENV_FILE_LINE);
        let env = pairs(&[("A", "1"), ("PEM", pem), ("LONG", &long), ("B", "2")]);
        let config = container_config(&container, &env).unwrap();

        assert_eq!(config.env_file, b"A=1\nB=2\n".to_vec());
        let mut expected = Vec::new();
        for part in ["PEM", pem, "LONG", &long] {
            expected.extend_from_slice(part.as_bytes());
            expected.push(0);
        }
        assert_eq!(config.secret_env, expected);
    }

    #[test]
    fn test_container_config_refuses_what_podman_cannot_hold() {
        let container = json!({"image": "x"});
        let too_big = "x".repeat(MAX_SECRET_ENV_VALUE + 1);
        for env in [
            pairs(&[("#COMMENT", "x")]),
            pairs(&[(" LEADING", "x")]),
            pairs(&[("A,B", "multi\nline")]),
            pairs(&[("BIG", &too_big)]),
        ] {
            assert!(
                container_config(&container, &env).is_err(),
                "{:?} must be refused",
                env[0].0
            );
        }
        // A comma is fine in a plain one-line variable.
        assert!(container_config(&container, &pairs(&[("A,B", "x")])).is_ok());
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

    #[test]
    fn test_no_resource_limits_means_no_flags() {
        let spec = container_spec(json!({"image": "busybox:latest"}));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "CPU_LIMIT=$(echo "), "");
        assert_eq!(decode_between(&script, "MEMORY_LIMIT=$(echo "), "");
    }

    #[test]
    fn test_whole_core_limit_has_no_decimal() {
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "resources": {"limits": {"cpu": "2", "memory": "4Gi"}}
        }));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "CPU_LIMIT=$(echo "), "2");
        assert_eq!(decode_between(&script, "MEMORY_LIMIT=$(echo "), "4096m");
    }

    #[test]
    fn test_fractional_cpu_limit_is_a_decimal_podman_understands() {
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "resources": {"limits": {"cpu": "500m", "memory": "512Mi"}}
        }));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "CPU_LIMIT=$(echo "), "0.5");
        assert_eq!(decode_between(&script, "MEMORY_LIMIT=$(echo "), "512m");
    }

    #[test]
    fn test_invalid_limit_quantity_does_not_block_script_generation() {
        // A real malformed resources.limits.cpu is rejected at admission
        // time (src/admission.rs's validate_resource_limits), before a
        // CronJob with one is ever accepted -- this shouldn't be
        // reachable in practice. This test covers the defense-in-depth
        // fallback here regardless: generate() itself must never panic
        // or fail a job run over a bad value that somehow reached it
        // (e.g. a row written before that admission check existed), it
        // just omits the flag, same as if the field had been left out.
        let spec = container_spec(json!({
            "image": "busybox:latest",
            "resources": {"limits": {"cpu": "not-a-quantity"}}
        }));
        let script = generate(&spec).unwrap();

        assert_eq!(decode_between(&script, "CPU_LIMIT=$(echo "), "");
    }
}
