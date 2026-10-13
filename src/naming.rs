//! Resource naming convention (Phase 16): `{resource_prefix}-{type}-{namespace}-{name}`.
//! Centralizes what `reconcile::job`'s `handle_volume_pending`/`handle_vm_pending`
//! (the title a resource is actually created with) and
//! `reconcile::orphan_scan` (the prefix it's later matched against) both
//! need to agree on, so the two sides can't independently drift.
//!
//! **`namespace` is included, not just `name`.** A job's own `name` is
//! only guaranteed unique *within* its namespace (`api::job::create_job`'s
//! own admission check, matching real Kubernetes' own per-namespace
//! uniqueness) -- two jobs named identically in different namespaces
//! previously generated the exact same UpCloud resource title. That's
//! not just a cosmetic collision: `handle_volume_pending`/
//! `handle_vm_pending`'s own Phase 11 idempotency check ("does a
//! resource titled exactly this already exist? reuse it instead of
//! creating a duplicate") would then find the *other* namespace's job's
//! resource and wrongly alias onto it -- two logically distinct jobs
//! silently sharing one real volume or worker VM. Found by re-checking
//! this convention against `api::job::create_job`'s own uniqueness
//! query (`WHERE name = ? AND namespace = ?`) while building this
//! module, not from an observed failure.
//!
//! One narrow case remains intentionally unguarded: a namespace/name
//! pair that's ambiguous against another pair purely by where an
//! embedded dash falls (e.g. namespace `"foo-bar"` + name `"baz"` vs.
//! namespace `"foo"` + name `"bar-baz"`). Closing that fully would mean
//! appending a per-job unique suffix (the job's own id) to every
//! title, at the cost of the title no longer being the readable string
//! `api::metrics`/`api::nodes` surface directly as a Kubernetes `Node`
//! name (`jobs.worker_vm_name`) -- not a trade worth making for
//! workloads this project actually runs, where namespace/name values
//! are chosen by the same operator, not adversarial input.

pub(crate) fn volume_title(resource_prefix: &str, namespace: &str, name: &str) -> String {
    format!("{resource_prefix}-vol-{namespace}-{name}")
}

pub(crate) fn worker_title(resource_prefix: &str, namespace: &str, name: &str) -> String {
    format!("{resource_prefix}-worker-{namespace}-{name}")
}

/// Matches any volume title `volume_title` could have produced for this
/// `resource_prefix`, regardless of namespace/name -- what
/// `reconcile::orphan_scan` needs ("is this resource plausibly ours at
/// all") before cross-checking it against the database.
pub(crate) fn volume_prefix(resource_prefix: &str) -> String {
    format!("{resource_prefix}-vol-")
}

pub(crate) fn worker_prefix(resource_prefix: &str) -> String {
    format!("{resource_prefix}-worker-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_title_includes_namespace_and_name() {
        assert_eq!(
            volume_title("kube-shim", "default", "foo"),
            "kube-shim-vol-default-foo"
        );
    }

    #[test]
    fn test_worker_title_includes_namespace_and_name() {
        assert_eq!(
            worker_title("kube-shim", "default", "foo"),
            "kube-shim-worker-default-foo"
        );
    }

    #[test]
    fn test_same_name_in_different_namespaces_no_longer_collides() {
        assert_ne!(
            volume_title("kube-shim", "default", "foo"),
            volume_title("kube-shim", "staging", "foo"),
        );
        assert_ne!(
            worker_title("kube-shim", "default", "foo"),
            worker_title("kube-shim", "staging", "foo"),
        );
    }

    #[test]
    fn test_prefixes_match_their_own_titles() {
        assert!(
            worker_title("kube-shim", "default", "foo").starts_with(&worker_prefix("kube-shim"))
        );
        assert!(
            volume_title("kube-shim", "default", "foo").starts_with(&volume_prefix("kube-shim"))
        );
    }

    #[test]
    fn test_different_resource_prefixes_do_not_share_a_title_prefix() {
        // What separates two independent kube-shim instances sharing one
        // UpCloud account (the plan's own original motivation for this
        // module) -- each instance's orphan scan must never match the
        // other's resources.
        assert!(!worker_title("kube-shim-work", "default", "foo")
            .starts_with(&worker_prefix("kube-shim")));
    }
}
