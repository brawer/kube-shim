//! `WorkloadKind` abstraction and shared per-job resource-sizing helpers
//! (Phase 5). Neither is wired into a live request path yet -- there's no
//! reconciliation loop or VM provisioning to consume them until Phase 6+
//! -- but both live here now, tested, as the single home later phases
//! build on rather than improvising their own version of each.

/// What kind of workload owns a job run. Only `CronJob` exists today.
/// `Deployment` is real Kubernetes' next logical workload type and is
/// deliberately not built yet (see docs/IMPLEMENTATION_PLAN.md "Future
/// Work: Deployments") -- this enum exists now purely so the
/// reconciliation and VM-provisioning code built on top of it later
/// (Phase 6+) isn't written in a way that silently assumes CronJob is the
/// only possible kind.
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
}
