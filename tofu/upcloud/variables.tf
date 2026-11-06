variable "hostname" {
  description = <<-EOT
    The public DNS name this instance will serve ACME-issued HTTPS on
    (config.toml's server.hostname). This module does NOT create the
    DNS record -- that's the calling config's job, using whatever DNS
    provider it has (see this module's own README). Point that record
    at this module's public_ipv4 output promptly after the first
    apply; until it resolves, kube-shim's cold-start ACME issuance
    fails and systemd keeps restarting it until it succeeds.
  EOT
  type        = string
}

variable "acme_contact_email" {
  description = "Optional contact email for the ACME account (config.toml's server.acme_contact_email). Let's Encrypt only ever uses this for expiry/problem notifications."
  type        = string
  default     = null
}

variable "acme_directory" {
  description = "\"staging\" (default, higher rate limits, untrusted cert chain -- for throwaway test instances) or \"production\". Changing it later is applied in place over SSH (see README.md's \"Updating in place\")."
  type        = string
  default     = "staging"
}

variable "zone" {
  description = "UpCloud zone to provision this instance (and the worker VMs/volumes it will later create) in, e.g. \"de-fra1\"."
  type        = string
  default     = "de-fra1"
}

variable "plan" {
  description = "UpCloud server plan for the kube-shim host itself (not the worker VMs it provisions on demand, which are sized per-job). The shim process itself is lightweight -- an HTTP server plus a SQLite database -- so the smallest real plan is enough; override only if you expect to track a very large number of concurrent jobs."
  type        = string
  default     = "1xCPU-1GB"
}

variable "operator_ssh_public_keys" {
  description = "SSH public keys allowed to log in to the host itself as the image's default admin user, for your own operational access (debugging, manual intervention). Separate from worker_ssh key material below, which this module generates itself for the shim's own use against the worker VMs it creates."
  type        = list(string)
}

variable "ssh_allowed_source_ipv4_ranges" {
  description = "IPv4 CIDR ranges allowed to reach the host's SSH port (22) at all. Defaults to open (0.0.0.0/0) -- tighten this to your own known IP ranges once you have them. This is a network-level filter only, evaluated before a connection is even accepted; it's independent of (and no substitute for) the SSH key authentication that still governs who can log in once a connection from an allowed range gets through."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "ssh_allowed_source_ipv6_ranges" {
  description = "Same as ssh_allowed_source_ipv4_ranges, for IPv6. Defaults to open (::/0)."
  type        = list(string)
  default     = ["::/0"]
}

variable "upcloud_runtime_token" {
  description = <<-EOT
    The UpCloud API token the shim itself uses at runtime to provision
    worker VMs/volumes (config.toml's [upcloud] token) -- a DIFFERENT
    credential from whatever token your OpenTofu/UpCloud *provider*
    block uses to create this host in the first place. UpCloud's own
    Terraform/OpenTofu provider has no resource for minting a
    scoped sub-account token, so this module can't generate one for
    you (verified against the provider's own resource list while
    building this module) -- create a dedicated UpCloud sub-account
    for this purpose yourself (UpCloud's control panel, "Account" ->
    "Users"), scoped to API access only, and pass its token here
    rather than reusing your own full-account token.
  EOT
  type        = string
  sensitive   = true
}

variable "dry_run" {
  description = "config.toml's [upcloud] dry_run. Leave true to stand up and verify this module itself without provisioning any real worker VMs/volumes yet."
  type        = bool
  default     = false
}

variable "resource_prefix" {
  description = "Prefix on every cloud resource this instance creates (config.toml's [shim] resource_prefix) -- lets more than one kube-shim instance share one UpCloud account without naming collisions or cross-instance orphan-scan interference. Also used as this host's own title/hostname prefix."
  type        = string
  default     = "kube-shim"
}

variable "main_currency" {
  description = "Currency cost figures are reported in (config.toml's [shim] main_currency), e.g. \"CHF\"."
  type        = string
  default     = "EUR"
}

variable "budget_daily_rate" {
  description = "Rolling budget accrual rate, in main_currency per day (config.toml's [shim] budget_daily_rate). 0 disables the budget guard entirely."
  type        = number
  default     = 0
}

variable "budget_rollover_cap_days" {
  description = "Caps how many days of unspent budget_daily_rate can accrue (config.toml's [shim] budget_rollover_cap_days)."
  type        = number
  default     = 7
}

variable "api_token_count" {
  description = <<-EOT
    How many bearer tokens to mint for the authenticated API
    (config.toml's [[server.api_tokens]]), each tracked as its own
    random_password resource/output. Rotation with an overlap window,
    the same way config.toml's own docs describe, now driven by
    `tofu apply` instead of SSH + a text editor: bump this to add a
    new token, migrate your kubernetes_provider/kubectl config to it
    (see the api_tokens output), then lower it back down to retire the
    old one(s) -- random_password's own indexing means raising the
    count only ever adds a new token at the end, never reshuffles an
    existing one's value. Each change is applied in place over SSH,
    keeping the server and its IP addresses -- see README.md's
    "Rotating credentials".
  EOT
  type        = number
  default     = 1
}

variable "reconciliation_interval_secs" {
  description = "config.toml's [reconciliation] interval_secs -- the fallback reconciliation tick, independent of the Notify-driven wake-ups a new CronJob/budget change triggers immediately."
  type        = number
  default     = 10
}

variable "image_tag" {
  description = <<-EOT
    Tag of ghcr.io/brawer/kube-shim to run. "latest" (default) keeps
    deploy/kube-shim.container's checked-in AutoUpdate=registry
    behavior, rolling forward on every release. An explicit release
    tag (e.g. "v0.3.3") pins that version and drops AutoUpdate, so the
    host only changes version when this value changes. Changing it
    updates the host in place over SSH (see README.md's "Updating in
    place"); it does not replace the server.
  EOT
  type        = string
  default     = "latest"

  validation {
    condition     = can(regex("^(latest|v[0-9]+\\.[0-9]+\\.[0-9]+)$", var.image_tag))
    error_message = "image_tag must be \"latest\" or a release tag such as \"v0.3.3\"."
  }
}

variable "operator_ssh_private_key" {
  description = <<-EOT
    Private key matching one of operator_ssh_public_keys, used by
    `tofu apply` to SSH into the host as its admin user and push
    config.toml and the quadlet unit (see README.md's "Updating in
    place"). Leave null (default) to use your running SSH agent
    instead. Only used for the connection, never stored on the host.
  EOT
  type        = string
  default     = null
  sensitive   = true
}
