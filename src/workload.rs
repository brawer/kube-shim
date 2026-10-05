//! `WorkloadKind` abstraction and shared per-job resource-sizing helpers
//! (Phase 5). Neither is wired into a live request path yet -- there's no
//! reconciliation loop or VM provisioning to consume them until Phase 6+
//! -- but both live here now, tested, as the single home later phases
//! build on rather than improvising their own version of each.

/// What kind of workload *owns* a job run, if any. Only `CronJob` exists
/// today. `Deployment` is real Kubernetes' next logical workload type
/// and is deliberately not built yet (see docs/IMPLEMENTATION_PLAN.md
/// "Future Work: Deployments") -- this enum exists now purely so the
/// reconciliation and VM-provisioning code built on top of it later
/// (Phase 6+) isn't written in a way that silently assumes CronJob is the
/// only possible kind.
///
/// Deliberately does *not* have a `Job` variant (Phase 13: standalone
/// `batch/v1` `Job`s, created directly rather than only ever spawned by
/// a `CronJob`'s schedule): this enum denotes the *owner*, not the job
/// run's own resource kind (which is always `Job` -- trivially, there's
/// no other option, so it wouldn't need an enum at all). A standalone
/// Job genuinely has no owner at all, the same as in real Kubernetes
/// (Jobs don't own other Jobs) -- that's `jobs.cronjob_name` being
/// absent, not a `WorkloadKind` value of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    CronJob,
}

/// A cloud-provider server plan big enough to satisfy a pod template's
/// resource requests. Real UpCloud plan names/specs (confirmed against
/// the actual API, not guessed) -- a small, deliberately incomplete
/// starting set, not a full catalog: osmdiffs' own profile (6 CPU/8GB)
/// already exceeds every plan listed here, and picking a real match for
/// that is Phase 9's job (VM provisioning), once it's actually launching
/// servers and can validate the choice hands-on the way every other
/// provider-specific decision in this plan has been.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPlan {
    pub name: &'static str,
    pub cpu_cores: u32,
    pub memory_gb: u32,
}

const SERVER_PLANS: &[ServerPlan] = &[
    ServerPlan {
        name: "DEV-1xCPU-1GB-10GB",
        cpu_cores: 1,
        memory_gb: 1,
    },
    ServerPlan {
        name: "DEV-1xCPU-2GB",
        cpu_cores: 1,
        memory_gb: 2,
    },
    ServerPlan {
        name: "DEV-1xCPU-4GB",
        cpu_cores: 1,
        memory_gb: 4,
    },
    ServerPlan {
        name: "DEV-2xCPU-4GB",
        cpu_cores: 2,
        memory_gb: 4,
    },
    ServerPlan {
        name: "DEV-2xCPU-8GB",
        cpu_cores: 2,
        memory_gb: 8,
    },
    ServerPlan {
        name: "DEV-2xCPU-16GB",
        cpu_cores: 2,
        memory_gb: 16,
    },
];

/// The cheapest plan (by cores, then memory) that meets or exceeds both
/// requested amounts, or `None` if nothing in the table is big enough --
/// callers decide what "no plan fits" means for them (Phase 9 would
/// presumably reject the job rather than silently under-provisioning it).
pub fn smallest_fitting_server_plan(cpu_cores: u32, memory_gb: u32) -> Option<&'static ServerPlan> {
    SERVER_PLANS
        .iter()
        .filter(|plan| plan.cpu_cores >= cpu_cores && plan.memory_gb >= memory_gb)
        .min_by_key(|plan| (plan.cpu_cores, plan.memory_gb))
}

/// Parses a Kubernetes CPU resource quantity (`resources.requests.cpu`,
/// e.g. `"2"` or `"500m"` for 500 millicores) into millicores, the same
/// integer-precision unit Kubernetes itself uses internally -- real
/// Kubernetes CPU requests are *not* restricted to whole cores (that's
/// standard k8s, not specific to Borg), and this project needs to
/// preserve that precision for accurate `kubectl top`/events reporting
/// (`api::metrics`/`api::nodes`), not just for VM sizing. `parse_cpu_cores`
/// below is a *separate, deliberately lossy* wrapper over this, used only
/// where a whole-core answer is actually required (`smallest_fitting_server_plan`:
/// UpCloud's own plan catalog only offers whole-CPU-count plans, so there's
/// no such thing as provisioning "0.5 CPU" regardless of what the job asked for).
pub fn parse_cpu_millicores(quantity: &str) -> Result<u32, String> {
    let quantity = quantity.trim();
    if let Some(millicores) = quantity.strip_suffix('m') {
        let millicores: f64 = millicores
            .parse()
            .map_err(|_| format!("not a valid CPU quantity: {quantity:?}"))?;
        if millicores < 0.0 {
            return Err(format!("CPU quantity must not be negative: {quantity:?}"));
        }
        return Ok(millicores.round() as u32);
    }

    let cores: f64 = quantity
        .parse()
        .map_err(|_| format!("not a valid CPU quantity: {quantity:?}"))?;
    if cores < 0.0 {
        return Err(format!("CPU quantity must not be negative: {quantity:?}"));
    }
    Ok((cores * 1000.0).round() as u32)
}

/// Rounds a CPU request *up* to a whole number of cores -- see
/// `parse_cpu_millicores`'s own docs for why this lossy rounding exists
/// as a separate function rather than being the only way to parse this
/// field: it's correct for sizing a worker VM (UpCloud has no
/// fractional-core plans), wrong for reporting what was actually asked
/// for.
pub fn parse_cpu_cores(quantity: &str) -> Result<u32, String> {
    Ok(parse_cpu_millicores(quantity)?.div_ceil(1000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smallest_fitting_plan_exact_match() {
        let plan = smallest_fitting_server_plan(2, 8).unwrap();
        assert_eq!(plan.name, "DEV-2xCPU-8GB");
    }

    #[test]
    fn test_smallest_fitting_plan_rounds_up() {
        // Nothing offers exactly 1 core / 3GB -- must round up to 4GB, not
        // pick a 2-core plan just because 2xCPU-4GB also has enough memory.
        let plan = smallest_fitting_server_plan(1, 3).unwrap();
        assert_eq!(plan.name, "DEV-1xCPU-4GB");
    }

    #[test]
    fn test_smallest_fitting_plan_minimal_request() {
        let plan = smallest_fitting_server_plan(1, 1).unwrap();
        assert_eq!(plan.name, "DEV-1xCPU-1GB-10GB");
    }

    #[test]
    fn test_smallest_fitting_plan_none_big_enough() {
        // osmdiffs' real profile (6 CPU / 8GB) -- deliberately exceeds
        // every plan in this starting table; see the module docs.
        assert!(smallest_fitting_server_plan(6, 8).is_none());
    }

    #[test]
    fn test_workload_kind_cronjob_exists() {
        let kind = WorkloadKind::CronJob;
        assert_eq!(kind, WorkloadKind::CronJob);
    }

    #[test]
    fn test_parse_cpu_cores_whole_number() {
        assert_eq!(parse_cpu_cores("2"), Ok(2));
    }

    #[test]
    fn test_parse_cpu_cores_millicores_rounds_up() {
        assert_eq!(parse_cpu_cores("500m"), Ok(1));
        assert_eq!(parse_cpu_cores("1500m"), Ok(2));
        assert_eq!(parse_cpu_cores("1000m"), Ok(1));
    }

    #[test]
    fn test_parse_cpu_cores_rejects_garbage() {
        assert!(parse_cpu_cores("not-a-number").is_err());
    }

    #[test]
    fn test_parse_cpu_cores_rejects_negative() {
        assert!(parse_cpu_cores("-2").is_err());
        assert!(parse_cpu_cores("-500m").is_err());
    }

    #[test]
    fn test_parse_cpu_millicores_preserves_fractional_precision() {
        assert_eq!(parse_cpu_millicores("500m"), Ok(500));
        assert_eq!(parse_cpu_millicores("1500m"), Ok(1500));
        assert_eq!(parse_cpu_millicores("2"), Ok(2000));
        assert_eq!(parse_cpu_millicores("1.5"), Ok(1500));
    }

    #[test]
    fn test_parse_cpu_millicores_rejects_negative() {
        assert!(parse_cpu_millicores("-500m").is_err());
    }
}
