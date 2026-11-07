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
  description = <<-EOT
    UpCloud server plan for the kube-shim host itself (not the worker
    VMs it provisions on demand, which are sized per job). The shim is
    an HTTP server plus a SQLite database, so STARTER-1xCPU-1GB (the
    default, about EUR 3.26/month, with a 10 GB disk) is always enough.
    The only other choice is "1xCPU-1GB", a legacy plan UpCloud no
    longer sells, at about EUR 8.26/month: keep it only for a host
    created with it (see README.md's "Plan and boot disk").
  EOT
  type        = string
  default     = "STARTER-1xCPU-1GB"

  validation {
    condition     = contains(["STARTER-1xCPU-1GB", "1xCPU-1GB"], var.plan)
    error_message = "plan must be \"STARTER-1xCPU-1GB\", or \"1xCPU-1GB\" for a host created with it."
  }
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

variable "api_tokens" {
  description = <<-EOT
    The bearer tokens to mint for the authenticated API (config.toml's
    [[server.api_tokens]]), keyed by a name of your choice, e.g. one per
    client. Each name gets its own random_password, so adding or removing
    one never changes any other token's value. To rotate: add a new
    name, move your client to it (see the api_tokens output), then
    remove the old name. Optionally give a token an RFC 3339 expires_at,
    e.g. "2027-01-01T00:00:00Z", after which kube-shim rejects it.
    Every change is pushed to the existing host over SSH; see README.md's
    "Rotating credentials".
  EOT
  type = map(object({
    expires_at = optional(string)
  }))
  default = { default = {} }

  validation {
    condition     = length(var.api_tokens) > 0
    error_message = "api_tokens needs at least one token: kube-shim refuses to start without one."
  }

  validation {
    condition = alltrue([
      for t in values(var.api_tokens) : t.expires_at == null || can(timeadd(t.expires_at, "0s"))
    ])
    error_message = "Each expires_at must be an RFC 3339 timestamp, e.g. \"2027-01-01T00:00:00Z\"."
  }

  validation {
    condition     = alltrue([for name in keys(var.api_tokens) : can(regex("^[A-Za-z0-9_.-]+$", name))])
    error_message = "Token names may only use letters, digits, '_', '.' and '-' (they're written into config.toml as comments)."
  }

  validation {
    condition = anytrue([
      for t in values(var.api_tokens) : t.expires_at == null || try(timecmp(t.expires_at, plantimestamp()) > 0, false)
    ])
    error_message = "At least one token must not have expired yet: kube-shim refuses to start if every token has expired."
  }
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
