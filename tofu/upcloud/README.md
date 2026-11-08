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

  # One bearer token per client, so each can be rotated on its own.
  # Leave this out to get a single token named "default".
  api_tokens = {
    terraform = {}
    laptop    = {}
  }
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
`kubectl`) at the result using one of the tokens, e.g.
`module.kube_shim.api_tokens["terraform"]`, as the bearer token, and define your real `CronJob`/`Secret` resources
against it the same way this project's own `docs/IMPLEMENTATION_PLAN.md`
testing sections do by hand with `curl`/`kubectl`.

## Updating in place

The server's `user_data` (cloud-init) only sets up a brand-new host.
The UpCloud provider marks `user_data` as `RequiresReplace`, so this
module tells OpenTofu to ignore later changes to it. Instead,
`terraform_data.push_config` copies `config.toml` and the quadlet unit
to the host over SSH whenever either one changes, and restarts
`kube-shim.service`. So changing `image_tag`, `api_tokens`,
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

On a brand-new host, this push is also what installs `config.toml` and
first starts `kube-shim.service`. cloud-init leaves `config.toml` out on
purpose (see "Secrets and `user_data`" below), so kube-shim only comes
up once the push lands, a few minutes into the same `tofu apply`.

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
disk (see "Plan and boot disk").
Changes to those in `cloud-init.sh.tftpl` only reach hosts created
after them. To rebuild on purpose:

```sh
tofu apply -replace=module.kube_shim.upcloud_server.kube_shim
```

That gives the host new public IP addresses (point DNS at the new
`public_ipv4`/`public_ipv6` outputs again) and loses its local data
(SQLite database, ACME cache), so do it while no worker VMs are
running, or they may be orphaned (see the Phase 16 manual orphan
cleanup).

## Plan and boot disk

The host runs on UpCloud's `STARTER-1xCPU-1GB` plan (1 vCPU, 1 GB)
with exactly the 10 GB `standard` disk the plan includes, so UpCloud
bills the disk as part of the plan (`part_of_plan: "yes"` in
`GET /1.3/server/{uuid}`). In `de-fra1` that's 0.4464 cents/h, about
EUR 3.26/month at 730 h (November 2026; check the price for your zone).
Neither the plan nor the disk is configurable: kube-shim needs little
(on the old plan, kube-shim.brawer.ch used 3.6 GB of disk and about
420 MB of memory), and a bigger disk or another tier would be billed
on top of the plan.

Up to v0.4.3 the module used the legacy `1xCPU-1GB` plan with a 25 GB
`maxiops` disk, at about EUR 8.26/month. UpCloud no longer sells that
plan (`current_offering: "no"` in `GET /1.3/plan`). If you ever change
the plan here, check that the new one has `current_offering: "yes"`.

### Moving a host created with v0.4.3 or earlier

Upgrading the module replaces such a host: a template disk can't
shrink, and its tier can't change in place. That means new IP
addresses, and the old disk is deleted with the old server. OpenTofu
destroys the old server before it creates the new one, so copy the
data off first:

1. Let running Jobs finish, so no worker VMs are left behind (they
   would be orphaned, see the Phase 16 manual orphan cleanup). Suspend
   your CronJobs, or move at a time none are due.
2. Stop kube-shim and copy its data directory (the SQLite database and
   the ACME cache) to your machine:

   ```sh
   ssh admin@OLD_IP sudo systemctl --user --machine=kube-shim@ stop kube-shim.service
   ssh admin@OLD_IP sudo tar -C /home/kube-shim -czpf - kube-shim-data > kube-shim-data.tgz
   ```

3. Bump the module's `source` and `tofu apply`. Check that the plan
   replaces only `upcloud_server.kube_shim` and the resources that
   depend on it.
4. Point DNS at the new `public_ipv4`/`public_ipv6` outputs (see
   "First apply: DNS").
5. Put the data back and restart:

   ```sh
   ssh admin@NEW_IP sudo systemctl --user --machine=kube-shim@ stop kube-shim.service
   ssh admin@NEW_IP sudo tar -C /home/kube-shim --numeric-owner -xzpf - < kube-shim-data.tgz
   ssh admin@NEW_IP sudo systemctl --user --machine=kube-shim@ start kube-shim.service
   ```

   Then check `https://YOUR_HOSTNAME/livez`, and resume your CronJobs.

Skip steps 2 and 5 if you don't need the old data. kube-shim then
starts with an empty database: re-apply your CronJobs and Secrets, and
it fetches a new certificate once DNS points at the new host.

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

Tokens are keyed by name in `api_tokens`, and each name has its own
`random_password`, so adding or removing one never changes another.
To rotate a token, say `terraform`:

1. Add a new name next to it, e.g. `terraform-2027 = {}`, and
   `tofu apply`. The new config is pushed in place (see "Updating in
   place"): no new server, no new IP addresses. Both tokens now work.
2. Move the client to `module.kube_shim.api_tokens["terraform-2027"]`.
3. Remove `terraform` from `api_tokens` and `tofu apply` again.

Instead of step 3, you can give the old token an end date, e.g.
`terraform = { expires_at = "2027-01-01T00:00:00Z" }`. kube-shim
checks `expires_at` on every request, so the token stops working at
that time without another apply. Remove it from `api_tokens` later to
tidy up. The module refuses a plan in which every token has already
expired, since kube-shim won't start without a valid one.

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

## Secrets and `user_data`

Anyone with API access to the UpCloud account can read a server's
`user_data`, and it stays there for the server's whole lifetime. So the
module keeps `config.toml`, with the bearer tokens, the runtime token and
the worker SSH private key, out of `user_data` and installs it over SSH
instead (see "Updating in place").

Hosts created with v0.3.x of this module or earlier still carry the
`config.toml` they were first created with in their `user_data`, since
changes to it are ignored. Upgrading the module doesn't change that, and
needs no extra step to keep working. To close the exposure, rotate the
credentials that were in it once (see "Rotating credentials"): every API
token from back then, `worker_ssh`, and `upcloud_runtime_token`.
Rebuilding the host (`-replace=...upcloud_server.kube_shim`) also clears
it, but the old values were readable until then, so rotating is still
the safer fix.

## Verified vs. not

The `STARTER-1xCPU-1GB` host ("Plan and boot disk") hasn't been
applied against a live account yet: on the first real apply, check
that its disk shows `part_of_plan: "yes"`. The data copy in "Moving a
host created with v0.4.3 or earlier" hasn't been run either.

Every step in `templates/cloud-init.sh.tftpl` up through writing
the quadlet unit is lifted directly from
`bootstrap/provision.sh`'s own already-hands-on-verified sequence (see
that script's comments). The one step this module adds on top --
non-interactively enabling `podman-auto-update.timer` via `machinectl
shell` right after `loginctl enable-linger` -- has **not** been
verified against a real, live `tofu apply` yet. If a first real apply stalls
anywhere, this is the first thing to check (`journalctl -u
cloud-init-local` / `/var/log/kube-shim-cloud-init.log` on the host).

The in-place push (`terraform_data.push_config` and
`templates/push-config.sh.tftpl`), which on a new host also writes the
first `config.toml` and starts kube-shim, hasn't run against a live
host yet either. It was checked with `tofu validate`, by rendering the script and
running `bash -n` on it, and by checking the image-tag substitution
against the checked-in unit file. Its assumptions most worth checking on
the first real apply: passwordless `sudo` for `admin`, and
`systemctl --user --machine=kube-shim@` reaching the service user's
systemd instance.
