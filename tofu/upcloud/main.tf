# Provisions a single kube-shim instance on UpCloud: the host itself
# (bootstrapped via cloud-init -- podman, the unprivileged-port sysctl,
# the dedicated service user and the quadlet unit -- then given its
# config.toml and started over SSH by terraform_data.push_config, in the
# same apply, with no manual follow-up needed beyond pointing DNS at the
# result), plus the credentials it needs, minted
# and tracked as real OpenTofu-managed state instead of hand-edited
# file contents over SSH:
#   - named bearer token(s) for the authenticated API (server.api_tokens)
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
#
# Keyed by name (var.api_tokens), so any one token can be added or
# retired without touching the others.
resource "random_password" "api_token" {
  for_each = var.api_tokens
  length   = 43
  special  = false
}

resource "tls_private_key" "worker_ssh" {
  algorithm = "ED25519"
}

locals {
  config_api_tokens = [for name, t in random_password.api_token : {
    name       = name
    token      = t.result
    expires_at = var.api_tokens[name].expires_at
  }]

  config_toml = templatefile("${path.module}/templates/config.toml.tftpl", {
    hostname              = var.hostname
    acme_directory        = var.acme_directory
    acme_contact_email    = var.acme_contact_email
    api_tokens            = local.config_api_tokens
    upcloud_runtime_token = var.upcloud_runtime_token
    dry_run               = var.dry_run
    zone                  = var.zone
    # Not a variable: config.rs's own docs confirm this UUID is a
    # public, account-independent UpCloud template shared across
    # every account/zone, not something a real deployment has ever
    # needed to override -- a knob with no real use isn't worth the
    # module's own surface area (flagged in review).
    worker_template_uuid = "01000000-0000-4000-8000-000030240200"
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

  # deploy/kube-shim.container is read verbatim from this repo (one copy,
  # shared with bootstrap/provision.sh's manual flow) and the image tag is
  # swapped in here, so the checked-in file stays usable as-is. A pinned
  # tag also drops AutoUpdate=registry, so podman-auto-update.timer
  # leaves that container alone.
  quadlet_unit_source = file("${path.module}/../../deploy/kube-shim.container")
  default_image_line  = "Image=${local.image_repository}:latest\n"
  image_repository    = "ghcr.io/brawer/kube-shim"
  image               = "${local.image_repository}:${var.image_tag}"
  quadlet_unit = var.image_tag == "latest" ? local.quadlet_unit_source : replace(
    replace(local.quadlet_unit_source, local.default_image_line, "Image=${local.image}\n"),
    "AutoUpdate=registry\n", ""
  )
}

# The storage each known plan includes, from UpCloud's GET /1.3/plan.
# Add a plan here only after checking it there.
locals {
  plan_storage = {
    "STARTER-1xCPU-1GB" = { size = 10, tier = "standard" }
    # Legacy (current_offering: "no"), for hosts created with it.
    "1xCPU-1GB" = { size = 25, tier = "maxiops" }
  }
  storage_size = var.storage_size != null ? var.storage_size : try(local.plan_storage[var.plan].size, null)
  storage_tier = var.storage_tier != null ? var.storage_tier : try(local.plan_storage[var.plan].tier, null)
}

resource "upcloud_server" "kube_shim" {
  hostname = "${var.resource_prefix}.local"
  title    = "${var.resource_prefix}-host"
  zone     = var.zone
  plan     = var.plan
  metadata = true
  firewall = true

  # Exactly the storage the plan includes, so UpCloud bills the disk as
  # part of the plan (part_of_plan: "yes"). A disk that differs in size
  # or tier is billed on top. A smaller disk or another tier replaces
  # the server: a template disk can't shrink, and its tier can't change
  # in place.
  template {
    storage = "Ubuntu Server 24.04 LTS (Noble Numbat)"
    size    = local.storage_size
    tier    = local.storage_tier
  }

  # Dual-stack: two separate "public" interfaces, not one interface
  # with two addresses -- confirmed against the upcloud provider's own
  # acceptance-test fixtures (ip_address_family defaults to IPv4 when
  # omitted; a second interface with it set to "IPv6" is the real,
  # working shape for a dual-stack public-facing server). Index 1/2
  # pinned explicitly so a later edit here can't accidentally reorder
  # which interface index[0]/index[1] below refer to.
  network_interface {
    type  = "public"
    index = 1
  }

  network_interface {
    type              = "public"
    index             = 2
    ip_address_family = "IPv6"
  }

  login {
    user              = "admin"
    keys              = var.operator_ssh_public_keys
    create_password   = false
    password_delivery = "none"
  }

  # user_data is RequiresReplace in the UpCloud provider, so it only
  # seeds a brand-new host and later changes to it are ignored. Updates
  # to config.toml or the quadlet unit reach an existing host through
  # terraform_data.push_config below instead, over SSH, without
  # replacing it. See README.md's "Updating in place".
  #
  # config.toml is deliberately not part of user_data: anyone with API
  # access to the UpCloud account can read a server's user_data, for the
  # server's whole lifetime, and config.toml holds the API bearer tokens,
  # the runtime token and the worker SSH private key. push_config
  # installs it over SSH instead, including on a brand-new host.
  user_data = templatefile("${path.module}/templates/cloud-init.sh.tftpl", {
    # base64, not embedded raw: avoids any escaping concerns across the
    # outer heredoc in cloud-init.sh.tftpl.
    quadlet_unit_content_base64 = base64encode(local.quadlet_unit)
  })

  lifecycle {
    ignore_changes = [user_data]

    precondition {
      condition     = local.storage_size != null && local.storage_tier != null
      error_message = "The module doesn't know the storage included in plan \"${var.plan}\". Set storage_size and storage_tier to that plan's storage_size and storage_tier (GET /1.3/plan, or `upctl server plans`)."
    }
  }
}

# UpCloud's DNS resolvers, one accept rule per resolver and protocol.
# Keep in step with UPCLOUD_DNS_RESOLVERS in src/reconcile/job.rs,
# which does the same for worker VMs.
locals {
  upcloud_dns_resolvers = [
    { family = "IPv4", address = "94.237.127.9" },
    { family = "IPv4", address = "94.237.40.9" },
    { family = "IPv6", address = "2a04:3540:53::1" },
    { family = "IPv6", address = "2a04:3544:53::1" },
  ]
  upcloud_dns_reply_rules = flatten([
    for resolver in local.upcloud_dns_resolvers : [
      for protocol in ["udp", "tcp"] : merge(resolver, { protocol = protocol })
    ]
  ])
}

resource "upcloud_firewall_rules" "kube_shim" {
  server_id = upcloud_server.kube_shim.id

  dynamic "firewall_rule" {
    for_each = var.ssh_allowed_source_ipv4_ranges
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
      comment              = "SSH management access (IPv4)"
    }
  }

  dynamic "firewall_rule" {
    for_each = var.ssh_allowed_source_ipv6_ranges
    content {
      action                 = "accept"
      direction              = "in"
      family                 = "IPv6"
      protocol               = "tcp"
      destination_port_start = "22"
      destination_port_end   = "22"
      source_address_start   = cidrhost(firewall_rule.value, 0)
      source_address_end     = cidrhost(firewall_rule.value, -1)
      comment                = "SSH management access (IPv6)"
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
    comment                = "Authenticated API + public status page (HTTPS, IPv4)"
  }

  firewall_rule {
    action                 = "accept"
    direction              = "in"
    family                 = "IPv6"
    protocol               = "tcp"
    destination_port_start = "443"
    destination_port_end   = "443"
    source_address_start   = "::"
    source_address_end     = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
    comment                = "Authenticated API + public status page (HTTPS, IPv6)"
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
    comment                = "ACME HTTP-01 challenge responder (IPv4)"
  }

  firewall_rule {
    action                 = "accept"
    direction              = "in"
    family                 = "IPv6"
    protocol               = "tcp"
    destination_port_start = "80"
    destination_port_end   = "80"
    source_address_start   = "::"
    source_address_end     = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
    comment                = "ACME HTTP-01 challenge responder (IPv6)"
  }

  # Replies from UpCloud's DNS resolvers (issue #138). UpCloud's
  # firewall is stateless, so the replies to the host's own DNS queries
  # are inbound packets like any other and need their own accept rule,
  # UDP and TCP from source port 53. Without these, systemd-resolved
  # still gets answers, but only through its TCP fallback after a UDP
  # timeout. Same resolvers in every zone:
  # https://upcloud.com/docs/guides/dns-servers/
  dynamic "firewall_rule" {
    for_each = local.upcloud_dns_reply_rules
    content {
      action               = "accept"
      direction            = "in"
      family               = firewall_rule.value.family
      protocol             = firewall_rule.value.protocol
      source_address_start = firewall_rule.value.address
      source_address_end   = firewall_rule.value.address
      source_port_start    = "53"
      source_port_end      = "53"
      comment              = "DNS replies from UpCloud resolver (${firewall_rule.value.protocol})"
    }
  }

  # "drop", not "deny" -- the only two valid `action` values the real
  # provider accepts are accept|drop (confirmed against its own schema
  # docs after review caught this; `tofu validate` doesn't catch a
  # wrong-but-well-typed enum string, since the schema itself types
  # this field as a plain, unconstrained string).
  firewall_rule {
    action    = "drop"
    direction = "in"
    family    = "IPv4"
    comment   = "Drop everything else inbound (IPv4)"
  }

  firewall_rule {
    action    = "drop"
    direction = "in"
    family    = "IPv6"
    comment   = "Drop everything else inbound (IPv6)"
  }
}

# Pushes config.toml and the quadlet unit to the host over SSH and
# restarts kube-shim.service, whenever either one changes, and once for
# a brand-new host. On a new host this push is what installs config.toml
# and first starts kube-shim.service, since cloud-init leaves both out to
# keep secrets out of user_data. It also lets config and image-tag
# changes land in place even though user_data can't change without
# replacing the server. See
# templates/push-config.sh.tftpl for what runs on the host.
resource "terraform_data" "push_config" {
  # Hashes, not the contents: config.toml holds secrets, and state only
  # needs to notice that something changed.
  triggers_replace = {
    server_id    = upcloud_server.kube_shim.id
    config_toml  = sha256(local.config_toml)
    quadlet_unit = sha256(local.quadlet_unit)
  }

  depends_on = [upcloud_firewall_rules.kube_shim]

  lifecycle {
    precondition {
      condition     = var.image_tag == "latest" || strcontains(local.quadlet_unit_source, local.default_image_line)
      error_message = "deploy/kube-shim.container no longer has an \"${trimspace(local.default_image_line)}\" line, so image_tag can't be substituted into it."
    }
  }

  connection {
    type        = "ssh"
    host        = upcloud_server.kube_shim.network_interface[0].ip_address
    user        = "admin"
    private_key = var.operator_ssh_private_key
    agent       = var.operator_ssh_private_key == null
    # A brand-new host needs a little while before sshd answers.
    timeout = "15m"
  }

  # Staging directory only the admin user can read: config.toml holds
  # the bearer tokens, the runtime token and the worker SSH key.
  provisioner "remote-exec" {
    inline = ["rm -rf /tmp/kube-shim-push && install -d -m 700 /tmp/kube-shim-push"]
  }

  provisioner "file" {
    content     = local.config_toml
    destination = "/tmp/kube-shim-push/config.toml"
  }

  provisioner "file" {
    content     = local.quadlet_unit
    destination = "/tmp/kube-shim-push/kube-shim.container"
  }

  provisioner "file" {
    content = templatefile("${path.module}/templates/push-config.sh.tftpl", {
      image = local.image
    })
    destination = "/tmp/kube-shim-push/apply.sh"
  }

  provisioner "remote-exec" {
    inline = ["bash /tmp/kube-shim-push/apply.sh"]
  }
}
