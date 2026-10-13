# kube-shim OpenTofu module

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

## What this module does NOT do

- **Create a DNS record.** UpCloud's own OpenTofu provider has nothing
  to do with DNS, and providers vary, so this is left to your root
  config -- point your own provider's record at this module's
  `public_ipv4` output.
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
  source = "github.com/brawer/kube-shim//tofu"

  hostname                 = "kube-shim.example.com"
  operator_ssh_public_keys = ["ssh-ed25519 AAAA... you@yourlaptop"]
  upcloud_runtime_token    = var.upcloud_runtime_token # a sub-account token, see above
  main_currency            = "CHF"
  budget_daily_rate        = 2.5
}

output "kube_shim_public_ip" {
  value = module.kube_shim.public_ipv4
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

## First apply: DNS is a two-step dance

`acme_directory` defaults to `"staging"` specifically so a first
`tofu apply` can succeed before DNS exists at all (staging issuance
doesn't require real-world reachability the same way; even so, the
very first `config.toml` written still has `hostname` set, so a cold
boot blocks on ACME regardless -- see `config.rs`'s own docs on cold-
start-vs-renewal failure handling). The real sequence:

1. `tofu apply` with `acme_directory = "staging"` and whatever
   `hostname` you intend to use.
2. Point your DNS record at the resulting `public_ipv4` output.
3. Once that resolves, set `acme_directory = "production"` and
   `tofu apply` again (this updates `user_data` in place rather than
   destroying/recreating the server, but whether that alone is
   enough to make the host pick up the new `config.toml` -- vs.
   needing an explicit reboot -- hasn't been verified; see the same
   caveat under "Rotating credentials" below).

## Rotating credentials

Bump `api_token_count`, `tofu apply` (adds a new token without
touching the existing one(s) -- `random_password`'s own indexing means
a higher count only ever appends), migrate your `kubernetes_provider`/
`kubectl` config to `module.kube_shim.api_tokens[<new index>]`, then
lower `api_token_count` back down and `tofu apply` once more to retire
the old one.

**Unverified: whether a `tofu apply` that only changes `user_data`
(as a token-count change does, through `config_toml_content`) alone
is enough, or whether the host needs an explicit reboot for
cloud-init to re-run against the new `user_data`.** UpCloud's own
provider docs don't say whether changing `user_data` on an existing,
running server actually re-triggers its execution, or only takes
effect from the next boot `cloud-init` would run anyway -- not
checked against a real apply yet. Until that's confirmed, treat a
credential rotation as needing a manual `upctl server restart` (or
the equivalent reboot through the control panel) right after
`tofu apply`, and confirm the new `config.toml` actually landed
(`journalctl -u kube-shim.service`) before relying on it.

There's no equivalent rotation knob for `worker_ssh`
(`tls_private_key`) or `upcloud_runtime_token` -- replacing either
means `tofu taint`/`-replace` on the `tls_private_key.worker_ssh`
resource, or simply passing a new `upcloud_runtime_token` value,
followed by an apply.

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
