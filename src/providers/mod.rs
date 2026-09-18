//! `CloudProvider`: the seam between the reconciliation loop and whichever
//! cloud actually provisions volumes/servers/firewalls (Phase 7).
//! `UpCloudProvider` (`src/providers/upcloud/`) is the only implementation
//! for now. This is a lightweight seam, not a finished multi-cloud
//! abstraction -- its exact method signatures should be expected to
//! change once a second provider (Hetzner or Infomaniak, see
//! docs/IMPLEMENTATION_PLAN.md "Future Work") is actually implemented
//! against it, rather than guessed correctly in advance.

pub mod upcloud;

use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProviderError {
    /// The provider rejected our credentials outright (HTTP 401/403).
    /// Kept distinct from `Api` so callers can react differently -- e.g.
    /// the startup connectivity check (`main.rs`) logs this as a clear,
    /// actionable warning rather than a generic failure.
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("provider API error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
}

#[derive(Debug, Clone)]
pub struct CreateVolumeRequest {
    pub size_gb: u32,
    /// Provider-specific tier name -- e.g. `StorageTier::upcloud_tier()`
    /// (Phase 5) for `UpCloudProvider`. Deliberately a plain string, not a
    /// shared enum: which tier names exist is entirely provider-specific
    /// (see `src/volumes.rs`'s own module docs on why this project
    /// doesn't try to unify that further).
    pub tier: String,
    pub title: String,
    pub zone: String,
}

#[derive(Debug, Clone)]
pub struct Volume {
    pub id: String,
    pub size_gb: u32,
    pub tier: String,
    pub title: String,
    pub zone: String,
}

#[derive(Debug, Clone)]
pub struct CreateServerRequest {
    pub title: String,
    pub hostname: String,
    pub zone: String,
    /// Provider-specific server plan name, e.g. `"DEV-1xCPU-1GB-10GB"`
    /// (see `src/workload.rs`'s `ServerPlan`, Phase 5).
    pub plan: String,
    /// UUID of the OS template to clone the boot disk from.
    pub template_uuid: String,
    /// Boot disk size in GB -- separate from any ephemeral scratch volume
    /// attached later via `attach_volume`.
    pub boot_disk_size_gb: u32,
    pub ssh_public_keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Server {
    pub id: String,
    pub title: String,
    /// Provider-native state string (e.g. `"maintenance"`, `"started"`,
    /// `"stopped"` for UpCloud) -- not normalized into a shared enum yet,
    /// since there's only one provider to normalize against.
    pub state: String,
    pub public_ipv4: Option<String>,
    pub public_ipv6: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirewallDirection {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirewallAction {
    Accept,
    Drop,
}

/// Mirrors UpCloud's own firewall rule shape closely (rather than
/// inventing a different abstraction) -- there's no second provider yet
/// to unify against, and the real API requires one rule per IP family, so
/// `family` is part of a single rule, not implied. `source_address`
/// covers this project's actual needs (an exact single address, e.g. the
/// shim's own VPS IP) -- serialized as a single-address range
/// (`source_address_start == source_address_end`) against UpCloud's own
/// range-shaped fields, which is a real, simpler subset of what the API
/// allows, not the full range feature.
#[derive(Debug, Clone)]
pub struct FirewallRule {
    pub direction: FirewallDirection,
    pub action: FirewallAction,
    pub family: FirewallFamily,
    pub protocol: Option<String>,
    pub source_address: Option<String>,
    pub destination_port_start: Option<String>,
    pub destination_port_end: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirewallFamily {
    Ipv4,
    Ipv6,
}

/// One entry from the provider's pricing catalog -- deliberately raw and
/// unnormalized (`amount`/`price` straight from UpCloud's own `GET
/// /1.3/price` response, in the account's billing currency, cents per
/// `amount` units) rather than a rich typed cost model: Phase 13 is where
/// that model actually gets designed, once ECB-rate conversion and budget
/// accrual exist to build it for. Committing to a shape now would just
/// mean redesigning it then.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceEntry {
    pub amount: f64,
    pub price: f64,
}

#[async_trait]
pub trait CloudProvider: Send + Sync {
    async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume, ProviderError>;
    async fn delete_volume(&self, volume_id: &str) -> Result<(), ProviderError>;
    async fn attach_volume(&self, server_id: &str, volume_id: &str) -> Result<(), ProviderError>;
    async fn detach_volume(&self, server_id: &str, volume_id: &str) -> Result<(), ProviderError>;

    async fn create_server(&self, req: CreateServerRequest) -> Result<Server, ProviderError>;
    /// For polling after `create_server`: UpCloud's own server creation is
    /// asynchronous (the response returns before the server is actually
    /// ready) -- verified against the real API reference, not assumed;
    /// see docs/IMPLEMENTATION_PLAN.md Phase 7. Phase 9 is what actually
    /// builds the poll loop; this method is the primitive it polls with.
    async fn get_server(&self, server_id: &str) -> Result<Server, ProviderError>;
    async fn delete_server(&self, server_id: &str) -> Result<(), ProviderError>;

    /// Replaces every firewall rule on the server with `rules`. UpCloud's
    /// own rule application is asynchronous *in effect*: this call
    /// returns once the API has accepted the rules, but they take
    /// roughly 1-2 minutes to actually start being enforced (verified
    /// against the real API reference). Phase 9 is what actually builds
    /// the wait/verify step before trusting a worker VM is unreachable;
    /// this method and `list_firewall_rules` below are the primitives it
    /// verifies with -- callers must not treat this call's return as
    /// proof the rules are already enforced.
    async fn create_firewall_rules(
        &self,
        server_id: &str,
        rules: &[FirewallRule],
    ) -> Result<(), ProviderError>;
    async fn list_firewall_rules(
        &self,
        server_id: &str,
    ) -> Result<Vec<FirewallRule>, ProviderError>;

    /// `price_key` is one of UpCloud's own pricing catalog keys verbatim
    /// (e.g. `"server_plan_DEV-1xCPU-1GB-10GB"`, `"storage_maxiops"`) --
    /// see `PriceEntry`'s own docs for why this stays a raw passthrough.
    async fn get_pricing(&self, zone: &str, price_key: &str) -> Result<PriceEntry, ProviderError>;
}
