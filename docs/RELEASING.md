# Cutting a release

This document is the practical how-to for cutting a release of `kube-shim`.

## Quick start

[`release-please`](https://github.com/googleapis/release-please) keeps a
"release PR" continuously open against `main`, titled something like
"chore(main): release 0.2.0", with `Cargo.toml`/`Cargo.lock`'s version bump
and the new `CHANGELOG.md` entries already prepared from merged PR titles.
To cut a release:

1. Open that PR, sanity-check the version number it proposes against
   "Choosing the version number" below, and merge it like any other PR (it
   goes through the normal required checks).
2. Once merged, `release-please` tags that commit and publishes the GitHub
   Release (with the same notes) on its own.
3. That tag push triggers
   [`release.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/release.yml)
   /
   [`release-build.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/release-build.yml),
   which builds `linux/amd64` and `linux/arm64` images, joins them into a
   multi-arch manifest published as both `ghcr.io/brawer/kube-shim:vX.Y.Z`
   and `ghcr.io/brawer/kube-shim:latest`, and generates SLSA Build Level 3
   provenance for every architecture-specific image and the manifest.
   Optionally verify the release actually came out right:
   ```sh
   gh attestation verify oci://ghcr.io/brawer/kube-shim:vX.Y.Z \
     --repo brawer/kube-shim
   ```

See "What happens automatically" below for the full sequence — but read
"Choosing the version number" first, it's the one part of this that
actually needs a careful decision, not just clicking merge.

## Choosing the version number

This is the one genuine judgment call in the whole process; everything
else is mechanical. We follow [SemVer](https://semver.org/), driven by
whether an **already-applied Terraform config or a running deployment**
keeps working — not by how much code changed.

Nobody links against `kube-shim` as a library; what matters is whether the
Kubernetes API surface it exposes, or `config.toml`'s schema, changed in a
way that breaks something already relying on it:

- **major** — the API surface or config schema changed in a way that
  breaks an existing Terraform config or deployed instance (a resource
  field was removed/renamed, an admission rule got stricter in a way that
  now rejects previously-valid manifests, a config.toml field was
  removed/renamed, ...)
- **minor** — the API surface or config schema evolved in a
  backward-compatible way (e.g. a new optional resource field or config
  option was added)
- **patch** — bugfixes only, no API/schema changes

A release that touches a lot of internal code but doesn't change the API
surface or config schema is still a patch release. A release that breaks
either in a small-looking diff is still a major release.

**Before 1.0.0, a breaking release bumps *minor*, not *major*.** SemVer's
own spec is explicit that this is fine: [§4](https://semver.org/#spec-item-4)
says a `0.y.z` major version is for initial development, where "anything
MAY change at any time" and the public interface "SHOULD NOT be considered
stable" — SemVer deliberately leaves how `0.y.z` itself increments up to
the project. We use the common convention of treating `0.MINOR.PATCH` the
way `MAJOR.MINOR.PATCH` works post-1.0: a breaking release bumps `MINOR`
(not `MAJOR`, which stays `0`), anything else bumps `PATCH`. This is also
the same rule `cargo`/crates.io itself uses for `0.x` dependency resolution
(a `^0.2.0` requirement excludes `0.3.0`, treating that minor-version bump
as the breaking one). We'll move to `MAJOR` bumps for breaking changes once
there's an actual 1.0.0 to break compatibility with — i.e. once this shim
has real deployments depending on API/schema stability, not before.

### How release-please's version bumps map to this rule

`release-please` derives version bumps from Conventional-Commits PR
titles, not from an API/schema diff — so the mapping in
[`release-please-config.json`](../release-please-config.json) has to
reproduce the two-tier, pre-1.0 rule above using only the commit types and
the `!` marker:

- `"bump-minor-pre-major": true` — a `!`-marked (breaking, per
  [`CONTRIBUTING.md`](CONTRIBUTING.md#pr-titles-conventional-commits))
  commit bumps **minor** instead of major, matching "before 1.0.0, a
  breaking release bumps minor" above.
- `"bump-patch-for-minor-pre-major": true` — a non-breaking `feat:` commit
  bumps **patch** instead of minor. Pre-1.0.0 there's no separate slot left
  for "API/schema evolved compatibly" once breaking has claimed minor —
  that's the whole point of the two-tier collapse, not a gap
  release-please forces on us.

Net effect while we're at `0.y.z`: a `!` commit → minor, everything else
(`feat:`, `fix:`, `chore:`, ...) → patch.

## What happens automatically

1. Every push to `main` (i.e. every merged PR) runs
   [`release-please.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/release-please.yml),
   which updates the standing release PR based on the Conventional-Commits
   PR titles merged since the last release.
2. Merging that release PR makes `release-please` tag the commit
   (`vX.Y.Z`) and publish the GitHub Release.
3. The tag push triggers `release.yml`, which calls `release-build.yml`:
   builds the `FROM scratch` image for `linux/amd64` and `linux/arm64`,
   pushes `ghcr.io/brawer/kube-shim:vX.Y.Z` and `:latest` as a multi-arch
   manifest, and generates SLSA Build Level 3 provenance (via
   `actions/attest`) for every architecture-specific image and the
   manifest.
4. The `kube-shim.brawer.ch` instance updates itself from here: its
   `deploy/kube-shim.container` quadlet unit tracks `:latest` with
   `AutoUpdate=registry`, and `podman-auto-update.timer` picks up the new
   image and restarts the service on its own, no SSH session needed. Per
   [`docs/IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) (Phase 3), this
   is a deliberate trade of update-safety for development-loop speed,
   judged acceptable specifically because UpCloud's prepaid, no-auto-recharge
   billing bounds the downside of an untested release running unattended. A
   deployment without that backstop should instead pin an explicit
   `vX.Y.Z` tag in the quadlet unit and update by hand with `podman pull` +
   `systemctl --user restart`.
