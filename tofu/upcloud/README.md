# kube-shim OpenTofu module (UpCloud)

Provisions a single, fully-bootstrapped `kube-shim` instance on UpCloud:
the host itself (podman, the unprivileged-port sysctl, the dedicated
service user, the quadlet unit, `config.toml`, and the running
service -- no manual SSH follow-up needed beyond pointing DNS at the
result), plus the credentials it needs, minted and tracked as real
OpenTofu state instead of hand-edited file contents (see
[GitHub issue #72](https://github.com/brawer/kube-shim/issues/72) for
the motivating discussion).

This is a **module**, meant to be called from your own root
configuration (e.g. a separate infrastructure repo that also defines
your real `CronJob`s against the instance this creates) -- it has no
`provider` blocks of its own, by Terraform/OpenTofu convention.

Lives at `tofu/upcloud/`, not `tofu/`, so a future module targeting a
different VPS provider can sit alongside it at `tofu/<provider>/`
without this one having to move later.

## What this module does NOT do

- **Create a DNS record.** UpCloud's own OpenTofu provider has nothing
  to do with DNS, and providers vary, so this is left to your root
  config -- point your own provider's A/AAAA records at this module's
  `public_ipv4`/`public_ipv6` outputs. The host itself is provisioned
  dual-stack regardless of whether you wire up both.
- **Mint a scoped UpCloud API token for the shim's own runtime use.**
  UpCloud's OpenTofu provider has no resource for creating a
  sub-account/API-scoped credential (checked against the provider's
  own resource list while building this module). Create a dedicated
  UpCloud sub-account yourself (control panel: Account -> Users),
  scoped to API access only, and pass its token as
  `var.upcloud_runtime_token` -- don't reuse the token your own
  `provider "upcloud" {}` block authenticates with to run `tofu
  apply` in the first place.

## Example usage

```hcl
terraform {
  required_providers {
    upcloud = { source = "UpCloudLtd/upcloud" }
  }
}

provider "upcloud" {
  # username/password, or UPCLOUD_USERNAME/UPCLOUD_PASSWORD env vars --
  # this is the account-level credential that creates the host itself,
  # separate from var.upcloud_runtime_token below.
}

module "kube_shim" {
  # The `//` is intentional, not a typo: Terraform/OpenTofu's own
  # module-source syntax for "this module lives in a subdirectory of
  # a larger repo" is `<repo-url>//<path-within-repo>` -- the part
  # before `//` is what actually gets cloned, the part after says
  # where inside it to look. A single slash would instead look for a
  # (nonexistent) GitHub repo literally named `kube-shim/tofu`.
  source = "github.com/brawer/kube-shim//tofu/upcloud"

  hostname                 = "kube-shim.example.com"
  operator_ssh_public_keys = ["ssh-ed25519 AAAA... you@yourlaptop"]
  upcloud_runtime_token    = var.upcloud_runtime_token # a sub-account token, see above
  main_currency            = "CHF"
  budget_daily_rate        = 2.5
}

output "kube_shim_public_ipv4" {
  value = module.kube_shim.public_ipv4
}

output "kube_shim_public_ipv6" {
  value = module.kube_shim.public_ipv6
}

output "kube_shim_api_tokens" {
  value     = module.kube_shim.api_tokens
  sensitive = true
}
```

Then, in the same root config, point a `kubernetes_provider` (or
`kubectl`) at the result using `module.kube_shim.api_tokens[0]` as the
bearer token, and define your real `CronJob`/`Secret` resources
against it the same way this project's own `docs/IMPLEMENTATION_PLAN.md`
testing sections do by hand with `curl`/`kubectl`.

## Every input change replaces the server

All of this module's inputs end up in the server's `user_data` (via
`config.toml.tftpl` and `cloud-init.sh.tftpl`), and the UpCloud
provider marks `user_data` as `RequiresReplace`. So changing any input
after the first apply (`acme_directory`, `api_token_count`, the
runtime token, budget settings, ...) **destroys and recreates the
host**, most likely with new public IP addresses. Plan for that: after
such an apply, point DNS at the new `public_ipv4`/`public_ipv6`
outputs again. The host's local data (SQLite database, ACME cache) does
not survive, so replace it while no worker VMs are running, or they may
be orphaned (see the Phase 16 manual orphan cleanup).

## First apply: DNS

Because flipping `acme_directory` later means a new server (see
above), set it to whatever you actually want from the very first
apply, normally `"production"`. The `"staging"` default is only meant
for throwaway test instances.

1. `tofu apply` with your real `hostname` and `acme_directory`.
2. Point your DNS record(s) at the resulting `public_ipv4`/`public_ipv6`
   outputs (an A record is enough; add the AAAA record too if you
   want DNS to actually prefer IPv6 for clients that support it --
   either way, the host answers HTTPS on both). Do this promptly.
3. Wait. Until DNS resolves to the new host, the first certificate
   can't be issued: `acme::setup()` gives up after its 5-minute
   cold-start timeout, the process exits, and systemd restarts it 10 s
   later (`Restart=on-failure` in `deploy/kube-shim.container`), so it
   keeps retrying on its own. Every failed attempt counts against
   Let's Encrypt's failed-validation limit (5 per hostname per hour),
   so a slow DNS update can delay issuance by up to an hour.

## Rotating credentials

Bump `api_token_count` and `tofu apply`. `random_password`'s own
indexing means a higher count only ever appends, so the existing
token(s) keep their values. But this changes `user_data`, which
**replaces the server** (see above): expect new IP addresses and a
fresh ACME issuance, and update DNS again. Then migrate your
`kubernetes_provider`/`kubectl` config to
`module.kube_shim.api_tokens[<new index>]`, lower `api_token_count`
back down, and `tofu apply` once more (another replacement) to retire
the old one. The overlap window this was designed for therefore
doesn't really exist yet. Avoiding the replacements would need a
stable address (e.g. an `upcloud_floating_ip_address`) or a way to
push config changes other than `user_data`, neither of which this
module has yet.

There's no equivalent rotation knob for `worker_ssh`
(`tls_private_key`) or `upcloud_runtime_token` -- replacing either
means `tofu taint`/`-replace` on the `tls_private_key.worker_ssh`
resource, or simply passing a new `upcloud_runtime_token` value,
followed by an apply (again replacing the server).

## State contains real secrets -- treat it accordingly

The minted bearer tokens, the worker SSH private key, and whatever
`upcloud_runtime_token` you pass in all land in OpenTofu state in
**plain text** -- that's how Terraform/OpenTofu state works for any
resource attribute, `sensitive = true` only affects CLI/log output,
not the state file itself. Use a remote backend with encryption at
rest (and don't commit `*.tfstate` -- see this repo's own
`.gitignore`), the same seriousness this project already applies to
`config.toml`/`secrets/` never reaching git history.

## Verified vs. not

Every step in `templates/cloud-init.sh.tftpl` up through writing
`config.toml` and the quadlet unit is lifted directly from
`bootstrap/provision.sh`'s own already-hands-on-verified sequence (see
that script's comments). The one step this module adds on top --
non-interactively starting `kube-shim.service` via `machinectl shell`
right after `loginctl enable-linger`, instead of a human doing it
manually a few seconds/minutes later -- has **not** been verified
against a real, live `tofu apply` yet. If a first real apply stalls
anywhere, this is the first thing to check (`journalctl -u
cloud-init-local` / `/var/log/kube-shim-cloud-init.log` on the host).
