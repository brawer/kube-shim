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

## Updating in place

The server's `user_data` (cloud-init) only sets up a brand-new host.
The UpCloud provider marks `user_data` as `RequiresReplace`, so this
module tells OpenTofu to ignore later changes to it. Instead,
`terraform_data.push_config` copies `config.toml` and the quadlet unit
to the host over SSH whenever either one changes, and restarts
`kube-shim.service`. So changing `image_tag`, `api_token_count`,
`acme_directory`, budget settings or the runtime token is one
`tofu apply` that keeps the server, its IP addresses, the SQLite
database and the ACME cache.

What this needs:

- SSH from wherever you run `tofu apply` to the host's public IPv4
  address, as the image's `admin` user. The key is the one in
  `operator_ssh_private_key`, or your SSH agent if that's left null.
  If you tighten `ssh_allowed_source_ipv4_ranges`, keep the machine
  you apply from inside it.
- `admin` needs passwordless `sudo`. UpCloud's login setup should
  give it that, but this hasn't been checked on a live host yet (see
  "Verified vs. not").

How a push runs (`templates/push-config.sh.tftpl`): wait for
first-boot cloud-init, pull the image (a bad tag fails the apply
before anything on the host changes), swap in `config.toml` and the
quadlet unit, restart, and fail the apply if the service doesn't come
back up. A failed push leaves `terraform_data.push_config` tainted, so
the next `tofu apply` simply tries again.

The SSH connection doesn't pin the host key, like OpenTofu
provisioners by default.

What still needs a new server: anything cloud-init itself sets up
(packages, sysctls, the service user), the zone, the plan, and the
disk. Changes to those in `cloud-init.sh.tftpl` only reach hosts
created after them. To rebuild on purpose:

```sh
tofu apply -replace=module.kube_shim.upcloud_server.kube_shim
```

That gives the host new public IP addresses (point DNS at the new
`public_ipv4`/`public_ipv6` outputs again) and loses its local data
(SQLite database, ACME cache), so do it while no worker VMs are
running, or they may be orphaned (see the Phase 16 manual orphan
cleanup).

## Pinning the image

`image_tag` defaults to `"latest"`, which keeps
`deploy/kube-shim.container`'s `AutoUpdate=registry` and rolls forward
on every release. Set a release tag to pin it:

```hcl
module "kube_shim" {
  # ...
  image_tag = "v0.3.3"
}
```

The module then rewrites the unit's `Image=` line and drops
`AutoUpdate=registry`. Bumping the tag later is an in-place update
(see above).

## First apply: DNS

Set `acme_directory` to what you actually want from the first apply,
normally `"production"`. The `"staging"` default is only meant for
throwaway test instances.

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
token(s) keep their values, and the new config is pushed in place (see
"Updating in place"): no new server, no new IP addresses. Then migrate
your `kubernetes_provider`/`kubectl` config to
`module.kube_shim.api_tokens[<new index>]` while both tokens work.

Retiring the old token is less tidy: lowering `api_token_count`
removes the *last* token, not the first, so it can't retire
`api_tokens[0]` on its own. Use
`tofu apply -replace='module.kube_shim.random_password.api_token[0]'`
to give index 0 a fresh value instead.

There's no equivalent rotation knob for `worker_ssh`
(`tls_private_key`) or `upcloud_runtime_token`. Replacing either means
`tofu apply -replace=module.kube_shim.tls_private_key.worker_ssh`, or
simply passing a new `upcloud_runtime_token` value. Both are pushed in
place like any other config change. Rotate `worker_ssh` while no worker
VMs are running, since running ones only accept the old key.

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

The in-place push (`terraform_data.push_config` and
`templates/push-config.sh.tftpl`) hasn't run against a live host yet
either. It was checked with `tofu validate`, by rendering the script and
running `bash -n` on it, and by checking the image-tag substitution
against the checked-in unit file. Its assumptions most worth checking on
the first real apply: passwordless `sudo` for `admin`, and
`systemctl --user --machine=kube-shim@` reaching the service user's
systemd instance.
