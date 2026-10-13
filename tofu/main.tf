# Provisions a single kube-shim instance on UpCloud: the host itself
# (fully bootstrapped via cloud-init -- podman, the unprivileged-port
# sysctl, the dedicated service user, the quadlet unit, config.toml,
# and the running service, with no manual follow-up needed beyond
# pointing DNS at the result), plus the credentials it needs, minted
# and tracked as real OpenTofu-managed state instead of hand-edited
# file contents over SSH:
#   - bearer token(s) for the authenticated API (server.api_tokens)
#   - an SSH keypair the shim itself uses against the worker VMs it
#     provisions (upcloud.worker_ssh_private_key/worker_ssh_public_keys)
#
# What this module deliberately does NOT do (see variables.tf's own
# docs for why): create a DNS record pointing at the host (provider-
# specific, left to the calling config), or mint a UpCloud API token
# scoped down from full account access (no resource for that exists in
# UpCloud's own OpenTofu provider -- supply var.upcloud_runtime_token
# from a sub-account you create yourself).

# Alphanumeric-only, not random_password's own default charset: these
# values are embedded into config.toml as plain double-quoted TOML
# strings (templates/config.toml.tftpl), where a literal `"` or `\`
# from a `special = true` character set would break the TOML syntax
# outright, not just look alarming. 43 alphanumeric characters is
# log2(62)*43 ~= 256 bits of entropy -- the same real strength as
# `openssl rand -base64 32` (this project's own existing convention,
# bootstrap/provision.sh), just over a safer charset.
resource "random_password" "api_token" {
  count   = var.api_token_count
  length  = 43
  special = false
}

resource "tls_private_key" "worker_ssh" {
  algorithm = "ED25519"
}

resource "upcloud_server" "kube_shim" {
  hostname = "${var.resource_prefix}.local"
  title    = "${var.resource_prefix}-host"
  zone     = var.zone
  plan     = var.plan
  metadata = true
  firewall = true

  template {
    storage = "Ubuntu Server 24.04 LTS (Noble Numbat)"
    size    = 25
  }

  network_interface {
    type = "public"
  }

  login {
    user              = "admin"
    keys              = var.operator_ssh_public_keys
    create_password   = false
    password_delivery = "none"
  }

  user_data = templatefile("${path.module}/templates/cloud-init.sh.tftpl", {
    config_toml_content = templatefile("${path.module}/templates/config.toml.tftpl", {
      hostname              = var.hostname
      acme_directory        = var.acme_directory
      acme_contact_email    = var.acme_contact_email
      api_tokens            = [for t in random_password.api_token : t.result]
      upcloud_runtime_token = var.upcloud_runtime_token
      dry_run               = var.dry_run
      zone                  = var.zone
      worker_template_uuid  = var.worker_template_uuid
      # trimspace: public_key_openssh has a trailing newline, which
      # would land inside a single-line TOML basic string
      # (worker_ssh_public_keys = ["..."]) -- a literal embedded
      # newline there is invalid TOML, not just untidy, and config.toml
      # would fail to parse at startup. Caught by actually rendering
      # this template and checking the output, not by inspection alone.
      worker_ssh_public_key        = trimspace(tls_private_key.worker_ssh.public_key_openssh)
      worker_ssh_private_key       = tls_private_key.worker_ssh.private_key_openssh
      reconciliation_interval_secs = var.reconciliation_interval_secs
      resource_prefix              = var.resource_prefix
      main_currency                = var.main_currency
      budget_daily_rate            = var.budget_daily_rate
      budget_rollover_cap_days     = var.budget_rollover_cap_days
    })
    # base64, not embedded raw: deploy/kube-shim.container is read
    # verbatim from this same repo (so there is exactly one copy of
    # it, never a second one drifting out of sync inside this
    # module) and passed through an *outer* heredoc in
    # cloud-init.sh.tftpl already, with no systemd-template syntax
    # of its own (`%h`, `%i`) needing to survive that; base64 sidesteps
    # any need to reason about escaping either layer.
    quadlet_unit_content_base64 = base64encode(file("${path.module}/../deploy/kube-shim.container"))
  })
}

resource "upcloud_firewall_rules" "kube_shim" {
  server_id = upcloud_server.kube_shim.id

  dynamic "firewall_rule" {
    for_each = var.ssh_allowed_source_ips
    content {
      action                 = "accept"
      direction              = "in"
      family                 = "IPv4"
      protocol               = "tcp"
      destination_port_start = "22"
      destination_port_end   = "22"
      # cidrhost(prefix, 0)/(prefix, -1): the first/last address in the
      # block -- NOT just the bare network address repeated for both
      # ends, which would silently narrow e.g. a /24 down to a single
      # host. For a /32 both calls agree, so this still covers the
      # common "just my one IP" case correctly too.
      source_address_start = cidrhost(firewall_rule.value, 0)
      source_address_end   = cidrhost(firewall_rule.value, -1)
      comment              = "SSH management access"
    }
  }

  firewall_rule {
    action                 = "accept"
    direction              = "in"
    family                 = "IPv4"
    protocol               = "tcp"
    destination_port_start = "443"
    destination_port_end   = "443"
    source_address_start   = "0.0.0.0"
    source_address_end     = "255.255.255.255"
    comment                = "Authenticated API + public status page (HTTPS)"
  }

  firewall_rule {
    action                 = "accept"
    direction              = "in"
    family                 = "IPv4"
    protocol               = "tcp"
    destination_port_start = "80"
    destination_port_end   = "80"
    source_address_start   = "0.0.0.0"
    source_address_end     = "255.255.255.255"
    comment                = "ACME HTTP-01 challenge responder"
  }

  firewall_rule {
    action    = "deny"
    direction = "in"
    family    = "IPv4"
    comment   = "Deny everything else inbound"
  }
}
