//! How a job's container is run on its worker VM, beyond image, env and
//! resources: the pod's and container's `securityContext`, and where the
//! scratch volume is mounted (issue #86).
//!
//! Supported:
//!
//! - `template.spec.securityContext.fsGroup`: the worker gives the
//!   scratch directory's root that group with the setgid bit
//!   (`chgrp <fsGroup> /scratch && chmod 2775 /scratch`, the same thing
//!   Kubernetes does to a volume's root), and the container process gets
//!   it as a supplementary group (`podman run --group-add`). The volume
//!   is always freshly formatted (or an empty boot-disk directory), so
//!   there is no existing data to chown recursively.
//! - `runAsUser`/`runAsGroup`, at pod level and on the container (the
//!   container's value wins): `podman run --user <uid>[:<gid>]`.
//!   `runAsGroup` without any `runAsUser` is rejected, since podman's
//!   `--user` can't set a group alone.
//! - `containers[0].securityContext.readOnlyRootFilesystem`:
//!   `podman run --read-only`. podman then still mounts tmpfs on `/tmp`,
//!   `/var/tmp`, `/run` and `/dev/shm` (its `--read-only-tmpfs` default).
//! - `runAsNonRoot`, at pod level and on the container (the container's
//!   value wins), checked like the kubelet does: a `runAsUser` of 0 is
//!   rejected at admission, and without any `runAsUser` the worker
//!   refuses to start the container unless the image's `USER` is a
//!   numeric, non-zero uid (an empty or named user can't be verified).
//! - `containers[0].volumeMounts[]` for the job's ephemeral volume:
//!   `mountPath` and `readOnly`. Without such a mount the volume stays
//!   at `/scratch`, as before.
//!
//! Like the env sources (issue #84), nothing here is silently dropped:
//! any other `securityContext` field, and any other option on the
//! scratch volume's mount (`subPath`, ...), is rejected at admission
//! (`validate`, called from `src/admission.rs`).
//!
//! The one exception is a few fields set to the value that matches what
//! kube-shim does anyway (issue #92): the OpenTofu/Terraform kubernetes
//! provider always sends them, even when the config never mentions them.
//! `privileged: false`, `allowPrivilegeEscalation: true` (Kubernetes'
//! default) and the scratch
//! mount's `mountPropagation: "None"` are accepted; any other value for
//! them is rejected. See the `*_NO_OP_VALUES` tables.
//!
//! The numbers and the mount path reach the worker's script base64-encoded
//! like every other job-supplied value (see `src/cloud_init.rs`), and are
//! validated here first: IDs must be integers in Kubernetes' own range
//! (0..=2147483647), and the mount path must be an absolute path podman's
//! `-v` can hold.

use serde_json::Value as JsonValue;

/// Where the worker mounts the scratch volume on the host, and where the
/// container sees it when no `volumeMounts[]` entry says otherwise.
pub const DEFAULT_SCRATCH_MOUNT_PATH: &str = "/scratch";

/// Kubernetes' own upper bound for `runAsUser`/`runAsGroup`/`fsGroup`.
const MAX_ID: u64 = 2_147_483_647;

const POD_SECURITY_CONTEXT_FIELDS: &[&str] =
    &["fsGroup", "runAsUser", "runAsGroup", "runAsNonRoot"];
const CONTAINER_SECURITY_CONTEXT_FIELDS: &[&str] = &[
    "runAsUser",
    "runAsGroup",
    "runAsNonRoot",
    "readOnlyRootFilesystem",
];
const SCRATCH_MOUNT_FIELDS: &[&str] = &["name", "mountPath", "readOnly"];

/// A value of an otherwise unsupported field that changes nothing, so
/// `validate` accepts it (issue #92).
#[derive(Debug, Clone, Copy)]
enum NoOp {
    Bool(bool),
    Str(&'static str),
}

impl NoOp {
    fn matches(self, value: &JsonValue) -> bool {
        match (self, value) {
            (NoOp::Bool(expected), JsonValue::Bool(b)) => expected == *b,
            (NoOp::Str(expected), JsonValue::String(s)) => expected == s,
            _ => false,
        }
    }
}

const POD_SECURITY_CONTEXT_NO_OP_VALUES: &[(&str, NoOp)] = &[];
const CONTAINER_SECURITY_CONTEXT_NO_OP_VALUES: &[(&str, NoOp)] = &[
    ("privileged", NoOp::Bool(false)),
    // Kubernetes' default: podman doesn't set no-new-privileges either.
    ("allowPrivilegeEscalation", NoOp::Bool(true)),
];
const SCRATCH_MOUNT_NO_OP_VALUES: &[(&str, NoOp)] = &[("mountPropagation", NoOp::Str("None"))];

/// A spec problem found at admission time: `field` is the offending
/// field's path relative to the pod spec (`template.spec`), e.g.
/// `securityContext.fsGroup`; `value` is the value being rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    pub field: String,
    pub value: String,
}

fn spec_error(field: impl Into<String>, value: impl Into<String>) -> SpecError {
    SpecError {
        field: field.into(),
        value: value.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOptions {
    /// `--group-add`, and the group of the scratch directory's root.
    pub fs_group: Option<u32>,
    /// `--user`: `"<uid>"` or `"<uid>:<gid>"`.
    pub user: Option<String>,
    /// `runAsNonRoot`: when `user` is `None`, the worker checks the
    /// image's `USER` before starting the container.
    pub run_as_non_root: bool,
    /// `--read-only`.
    pub read_only_root_filesystem: bool,
    /// `-v`'s argument for the scratch volume, e.g. `/scratch:/workdir`
    /// or `/scratch:/scratch:ro`.
    pub scratch_volume: String,
}

/// Admission-time check: every value `parse` reads is valid, and nothing
/// kube-shim would ignore is set.
pub fn validate(job_spec: &JsonValue) -> Result<(), SpecError> {
    let pod_spec = job_spec.pointer("/template/spec");
    if let Some(sc) = pod_spec.and_then(|p| p.get("securityContext")) {
        reject_unknown_fields(
            sc,
            POD_SECURITY_CONTEXT_FIELDS,
            POD_SECURITY_CONTEXT_NO_OP_VALUES,
            "securityContext",
        )?;
    }
    if let Some(sc) = job_spec.pointer("/template/spec/containers/0/securityContext") {
        reject_unknown_fields(
            sc,
            CONTAINER_SECURITY_CONTEXT_FIELDS,
            CONTAINER_SECURITY_CONTEXT_NO_OP_VALUES,
            "containers[0].securityContext",
        )?;
    }
    if let Some((i, mount)) = scratch_mount(job_spec) {
        reject_unknown_fields(
            mount,
            SCRATCH_MOUNT_FIELDS,
            SCRATCH_MOUNT_NO_OP_VALUES,
            &format!("containers[0].volumeMounts[{i}]"),
        )?;
    }
    parse(job_spec).map(|_| ())
}

/// The run options for `job_spec` (a Job's `.spec`, or a CronJob's
/// `.spec.jobTemplate.spec`). Fields `validate` would reject as unknown
/// are ignored here; an invalid value for a supported field is an error.
pub fn parse(job_spec: &JsonValue) -> Result<RunOptions, SpecError> {
    let pod_sc = job_spec.pointer("/template/spec/securityContext");
    let container_sc = job_spec.pointer("/template/spec/containers/0/securityContext");

    let fs_group = id_field(pod_sc, "fsGroup", "securityContext")?;

    let pod_user = id_field(pod_sc, "runAsUser", "securityContext")?;
    let pod_group = id_field(pod_sc, "runAsGroup", "securityContext")?;
    let container_user = id_field(container_sc, "runAsUser", "containers[0].securityContext")?;
    let container_group = id_field(container_sc, "runAsGroup", "containers[0].securityContext")?;
    let user = match (container_user.or(pod_user), container_group.or(pod_group)) {
        (None, None) => None,
        (Some(uid), None) => Some(uid.to_string()),
        (Some(uid), Some(gid)) => Some(format!("{uid}:{gid}")),
        (None, Some(gid)) => {
            let field = if container_group.is_some() {
                "containers[0].securityContext.runAsGroup"
            } else {
                "securityContext.runAsGroup"
            };
            return Err(spec_error(
                field,
                format!("{gid} (runAsGroup is only supported together with runAsUser)"),
            ));
        }
    };

    let pod_non_root = bool_field(pod_sc, "runAsNonRoot", "securityContext")?;
    let container_non_root = bool_field(
        container_sc,
        "runAsNonRoot",
        "containers[0].securityContext",
    )?;
    let run_as_non_root = container_non_root.or(pod_non_root).unwrap_or(false);
    if run_as_non_root && container_user.or(pod_user) == Some(0) {
        let field = if container_user.is_some() {
            "containers[0].securityContext.runAsUser"
        } else {
            "securityContext.runAsUser"
        };
        return Err(spec_error(field, "0 (runAsNonRoot is set)"));
    }

    let read_only_root_filesystem = bool_field(
        container_sc,
        "readOnlyRootFilesystem",
        "containers[0].securityContext",
    )?
    .unwrap_or(false);

    let scratch_volume = match scratch_mount(job_spec) {
        None => format!("{DEFAULT_SCRATCH_MOUNT_PATH}:{DEFAULT_SCRATCH_MOUNT_PATH}"),
        Some((i, mount)) => {
            let field = format!("containers[0].volumeMounts[{i}]");
            let path = match mount.get("mountPath") {
                Some(JsonValue::String(path)) => path,
                other => {
                    return Err(spec_error(
                        format!("{field}.mountPath"),
                        other.map(JsonValue::to_string).unwrap_or_default(),
                    ))
                }
            };
            if !is_valid_mount_path(path) {
                return Err(spec_error(format!("{field}.mountPath"), path.as_str()));
            }
            let read_only = match mount.get("readOnly") {
                None | Some(JsonValue::Null) => false,
                Some(JsonValue::Bool(b)) => *b,
                Some(other) => {
                    return Err(spec_error(format!("{field}.readOnly"), other.to_string()))
                }
            };
            let suffix = if read_only { ":ro" } else { "" };
            format!("{DEFAULT_SCRATCH_MOUNT_PATH}:{path}{suffix}")
        }
    };

    Ok(RunOptions {
        fs_group,
        user,
        run_as_non_root,
        read_only_root_filesystem,
        scratch_volume,
    })
}

/// The first container's `volumeMounts[]` entry (and its index) for the
/// job's ephemeral volume: the first `volumes[]` entry with `ephemeral`,
/// the same one `reconcile::job::find_ephemeral_volume` creates.
fn scratch_mount(job_spec: &JsonValue) -> Option<(usize, &JsonValue)> {
    let volume_name = job_spec
        .pointer("/template/spec/volumes")?
        .as_array()?
        .iter()
        .find(|v| v.get("ephemeral").is_some())?
        .get("name")?
        .as_str()?;
    job_spec
        .pointer("/template/spec/containers/0/volumeMounts")?
        .as_array()?
        .iter()
        .enumerate()
        .find(|(_, m)| m.get("name").and_then(JsonValue::as_str) == Some(volume_name))
}

/// Rejects any key of `value` that is neither in `supported` nor, with
/// a matching value (or `null`), in `no_ops`. An unknown key is reported
/// as the value of `field`; a no-op key with another value is reported
/// as `field.key` with that value.
fn reject_unknown_fields(
    value: &JsonValue,
    supported: &[&str],
    no_ops: &[(&str, NoOp)],
    field: &str,
) -> Result<(), SpecError> {
    let map = match value {
        JsonValue::Null => return Ok(()),
        JsonValue::Object(map) => map,
        other => return Err(spec_error(field, other.to_string())),
    };
    for (key, v) in map {
        if supported.contains(&key.as_str()) {
            continue;
        }
        match no_ops.iter().find(|(name, _)| name == key) {
            None => return Err(spec_error(field, key.as_str())),
            Some((_, no_op)) if !v.is_null() && !no_op.matches(v) => {
                return Err(spec_error(format!("{field}.{key}"), v.to_string()))
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn id_field(
    security_context: Option<&JsonValue>,
    key: &str,
    prefix: &str,
) -> Result<Option<u32>, SpecError> {
    match security_context.and_then(|sc| sc.get(key)) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|id| *id <= MAX_ID)
            .map(|id| Some(id as u32))
            .ok_or_else(|| spec_error(format!("{prefix}.{key}"), value.to_string())),
    }
}

fn bool_field(
    security_context: Option<&JsonValue>,
    key: &str,
    prefix: &str,
) -> Result<Option<bool>, SpecError> {
    match security_context.and_then(|sc| sc.get(key)) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(b)) => Ok(Some(*b)),
        Some(other) => Err(spec_error(format!("{prefix}.{key}"), other.to_string())),
    }
}

/// Absolute, not `/` itself, no `.`/`..` components, and nothing podman's
/// `-v src:dst[:options]` syntax would misread (`:`) or a C string can't
/// hold (NUL).
fn is_valid_mount_path(path: &str) -> bool {
    path.starts_with('/')
        && path != "/"
        && !path.contains([':', '\0', '\n'])
        && !path.split('/').any(|part| part == "." || part == "..")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job_spec(pod: JsonValue) -> JsonValue {
        json!({"template": {"spec": pod}})
    }

    /// The osmdiffs CronJob from issue #86.
    fn osmdiffs() -> JsonValue {
        job_spec(json!({
            "securityContext": {"fsGroup": 1000},
            "containers": [{
                "image": "x",
                "securityContext": {"readOnlyRootFilesystem": true},
                "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]
            }],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
        }))
    }

    #[test]
    fn test_defaults_when_nothing_is_set() {
        let spec = job_spec(json!({"containers": [{"image": "x"}]}));
        assert!(validate(&spec).is_ok());
        assert_eq!(
            parse(&spec).unwrap(),
            RunOptions {
                fs_group: None,
                user: None,
                run_as_non_root: false,
                read_only_root_filesystem: false,
                scratch_volume: "/scratch:/scratch".to_string(),
            }
        );
    }

    #[test]
    fn test_osmdiffs_cronjob() {
        let spec = osmdiffs();
        assert!(validate(&spec).is_ok());
        let options = parse(&spec).unwrap();
        assert_eq!(options.fs_group, Some(1000));
        assert!(options.read_only_root_filesystem);
        assert_eq!(options.user, None);
        assert_eq!(options.scratch_volume, "/scratch:/scratch");
    }

    /// What the OpenTofu kubernetes provider (v3.5.0) sends for the
    /// osmdiffs CronJob in brawer/production (issue #92): the HCL only
    /// sets `fs_group`, `read_only_root_filesystem` and one
    /// `volume_mount`, the rest are the provider's no-op defaults.
    #[test]
    fn test_opentofu_provider_defaults_accepted() {
        let spec = job_spec(json!({
            "securityContext": {"fsGroup": 1000, "runAsNonRoot": false},
            "containers": [{
                "image": "x",
                "securityContext": {
                    "allowPrivilegeEscalation": true,
                    "privileged": false,
                    "readOnlyRootFilesystem": true,
                    "runAsNonRoot": false
                },
                "volumeMounts": [{"name": "scratch", "mountPath": "/scratch", "mountPropagation": "None"}]
            }],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
        }));
        assert_eq!(validate(&spec), Ok(()));
        let options = parse(&spec).unwrap();
        assert_eq!(options.fs_group, Some(1000));
        assert!(options.read_only_root_filesystem);
        assert_eq!(options.user, None);
        assert_eq!(options.scratch_volume, "/scratch:/scratch");
    }

    #[test]
    fn test_no_op_fields_null_accepted() {
        let spec = job_spec(json!({
            "securityContext": {"runAsNonRoot": null},
            "containers": [{
                "image": "x",
                "securityContext": {"privileged": null, "allowPrivilegeEscalation": null},
                "volumeMounts": [{"name": "scratch", "mountPath": "/s", "mountPropagation": null}]
            }],
            "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
        }));
        assert_eq!(validate(&spec), Ok(()));
    }

    #[test]
    fn test_no_op_fields_with_other_values_rejected() {
        let cases = [
            (
                json!({"containers": [{"image": "x", "securityContext": {"privileged": true}}]}),
                spec_error("containers[0].securityContext.privileged", "true"),
            ),
            (
                json!({"containers": [{"image": "x", "securityContext": {"allowPrivilegeEscalation": false}}]}),
                spec_error(
                    "containers[0].securityContext.allowPrivilegeEscalation",
                    "false",
                ),
            ),
            (
                json!({"containers": [{"image": "x", "securityContext": {"privileged": "false"}}]}),
                spec_error("containers[0].securityContext.privileged", "\"false\""),
            ),
            (
                json!({
                    "containers": [{"image": "x", "volumeMounts": [
                        {"name": "scratch", "mountPath": "/s", "mountPropagation": "Bidirectional"}
                    ]}],
                    "volumes": [{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
                }),
                spec_error(
                    "containers[0].volumeMounts[0].mountPropagation",
                    "\"Bidirectional\"",
                ),
            ),
        ];
        for (pod, expected) in cases {
            assert_eq!(validate(&job_spec(pod.clone())), Err(expected), "{pod}");
        }
    }

    #[test]
    fn test_run_as_user_and_group_container_overrides_pod() {
        let spec = job_spec(json!({
            "securityContext": {"runAsUser": 1, "runAsGroup": 2},
            "containers": [{"image": "x", "securityContext": {"runAsUser": 1000}}]
        }));
        assert!(validate(&spec).is_ok());
        assert_eq!(parse(&spec).unwrap().user.as_deref(), Some("1000:2"));

        let spec = job_spec(json!({
            "securityContext": {"runAsUser": 0},
            "containers": [{"image": "x"}]
        }));
        assert_eq!(parse(&spec).unwrap().user.as_deref(), Some("0"));
    }

    #[test]
    fn test_run_as_non_root() {
        // Off unless set; the container's value wins over the pod's.
        let spec = job_spec(json!({"containers": [{"image": "x"}]}));
        assert!(!parse(&spec).unwrap().run_as_non_root);
        let spec = job_spec(json!({
            "securityContext": {"runAsNonRoot": true},
            "containers": [{"image": "x"}]
        }));
        assert_eq!(validate(&spec), Ok(()));
        assert!(parse(&spec).unwrap().run_as_non_root);
        let spec = job_spec(json!({
            "securityContext": {"runAsNonRoot": true},
            "containers": [{"image": "x", "securityContext": {"runAsNonRoot": false, "runAsUser": 0}}]
        }));
        assert_eq!(validate(&spec), Ok(()));
        assert!(!parse(&spec).unwrap().run_as_non_root);

        // A non-zero runAsUser satisfies it, at either level.
        let spec = job_spec(json!({
            "securityContext": {"runAsUser": 1000},
            "containers": [{"image": "x", "securityContext": {"runAsNonRoot": true}}]
        }));
        assert_eq!(validate(&spec), Ok(()));
        let options = parse(&spec).unwrap();
        assert!(options.run_as_non_root);
        assert_eq!(options.user.as_deref(), Some("1000"));
    }

    #[test]
    fn test_run_as_non_root_with_root_user_rejected() {
        let spec = job_spec(json!({
            "securityContext": {"runAsNonRoot": true, "runAsUser": 0},
            "containers": [{"image": "x"}]
        }));
        assert_eq!(
            validate(&spec),
            Err(spec_error(
                "securityContext.runAsUser",
                "0 (runAsNonRoot is set)"
            ))
        );
        let spec = job_spec(json!({
            "securityContext": {"runAsNonRoot": true, "runAsUser": 1000},
            "containers": [{"image": "x", "securityContext": {"runAsUser": 0}}]
        }));
        assert_eq!(
            validate(&spec),
            Err(spec_error(
                "containers[0].securityContext.runAsUser",
                "0 (runAsNonRoot is set)"
            ))
        );
        let spec = job_spec(json!({
            "containers": [{"image": "x", "securityContext": {"runAsNonRoot": "yes"}}]
        }));
        assert_eq!(
            validate(&spec),
            Err(spec_error(
                "containers[0].securityContext.runAsNonRoot",
                "\"yes\""
            ))
        );
    }

    #[test]
    fn test_run_as_group_without_user_rejected() {
        let spec = job_spec(json!({
            "containers": [{"image": "x", "securityContext": {"runAsGroup": 1000}}]
        }));
        let err = validate(&spec).unwrap_err();
        assert_eq!(err.field, "containers[0].securityContext.runAsGroup");
    }

    #[test]
    fn test_invalid_ids_rejected() {
        for bad in [
            json!(-1),
            json!("1000"),
            json!(1.5),
            json!(2_147_483_648u64),
        ] {
            let spec = job_spec(json!({
                "securityContext": {"fsGroup": bad.clone()},
                "containers": [{"image": "x"}]
            }));
            let err = validate(&spec).unwrap_err();
            assert_eq!(err.field, "securityContext.fsGroup", "{bad}");
            assert!(parse(&spec).is_err(), "{bad}");
        }
        let spec = job_spec(json!({
            "securityContext": {"fsGroup": 2_147_483_647u64},
            "containers": [{"image": "x"}]
        }));
        assert_eq!(parse(&spec).unwrap().fs_group, Some(2_147_483_647));
    }

    #[test]
    fn test_unsupported_security_context_fields_rejected() {
        let spec = job_spec(json!({
            "securityContext": {"fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [{"image": "x"}]
        }));
        assert_eq!(
            validate(&spec).unwrap_err(),
            spec_error("securityContext", "seccompProfile")
        );
        // parse() itself tolerates unknown fields (see its docs).
        assert_eq!(parse(&spec).unwrap().fs_group, Some(1000));

        let spec = job_spec(json!({
            "containers": [{"image": "x", "securityContext": {"capabilities": {"add": ["NET_ADMIN"]}}}]
        }));
        assert_eq!(
            validate(&spec).unwrap_err(),
            spec_error("containers[0].securityContext", "capabilities")
        );

        // fsGroup is pod-level only, as in Kubernetes.
        let spec = job_spec(json!({
            "containers": [{"image": "x", "securityContext": {"fsGroup": 1000}}]
        }));
        assert!(validate(&spec).is_err());
    }

    #[test]
    fn test_read_only_root_filesystem_must_be_a_bool() {
        let spec = job_spec(json!({
            "containers": [{"image": "x", "securityContext": {"readOnlyRootFilesystem": "true"}}]
        }));
        assert!(validate(&spec).is_err());
    }

    #[test]
    fn test_scratch_mount_path_and_read_only_honored() {
        let spec = job_spec(json!({
            "containers": [{"image": "x", "volumeMounts": [
                {"name": "other", "mountPath": "/ignored"},
                {"name": "work", "mountPath": "/workdir", "readOnly": true}
            ]}],
            "volumes": [
                {"name": "other", "secret": {"secretName": "s"}},
                {"name": "work", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}
            ]
        }));
        assert!(validate(&spec).is_ok());
        assert_eq!(parse(&spec).unwrap().scratch_volume, "/scratch:/workdir:ro");
    }

    #[test]
    fn test_bad_scratch_mounts_rejected() {
        for mount in [
            json!({"name": "work", "mountPath": "relative"}),
            json!({"name": "work", "mountPath": "/"}),
            json!({"name": "work", "mountPath": "/a:/b"}),
            json!({"name": "work", "mountPath": "/a/../etc"}),
            json!({"name": "work"}),
            json!({"name": "work", "mountPath": "/w", "readOnly": "yes"}),
            json!({"name": "work", "mountPath": "/w", "subPath": "x"}),
        ] {
            let spec = job_spec(json!({
                "containers": [{"image": "x", "volumeMounts": [mount.clone()]}],
                "volumes": [{"name": "work", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]
            }));
            let err = validate(&spec).unwrap_err();
            assert!(
                err.field.starts_with("containers[0].volumeMounts[0]"),
                "{mount}: {err:?}"
            );
        }
    }
}
