# Contributing

Thanks for taking a look at `kube-shim`! 👋 Contributions, questions, and bug
reports are all welcome — this project is still early, so there's no such
thing as too small a
[Pull Request (PR)](https://docs.github.com/en/pull-requests/get-started/about-pull-requests).

## Before opening a Pull Request

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

If your change touches server startup, TLS, or the container image, also
run the smoke test against the real compiled binary (not just `cargo test`,
which never exercises actual binary startup or config-file parsing):

```sh
cargo build --release
./smoke-test.sh --skip-build
```

After you've sent a Pull Request, CI
([`ci.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/ci.yml))
runs these same checks. Running them locally first saves a round-trip
through CI.

## PR titles: Conventional Commits

PRs are squash-merged, and the PR title becomes the commit message on
`main` — so give it the shape of a
[Conventional Commit](https://www.conventionalcommits.org/):

```
<type>[optional scope][!]: <description>
```

`type` is one of `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
`build`, `ci`, or `chore`. A bot
([`pr-title-lint.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/pr-title-lint.yml))
checks this on every PR and applies an `enhancement`/`bug`/`documentation`/
`breaking-change` label from it, which
[`release-please.yml`](https://github.com/brawer/kube-shim/blob/main/.github/workflows/release-please.yml)
reads to prepare the next version bump and `CHANGELOG.md` entry — so
getting the type right isn't just a style nit, it drives the release
automation directly.

**The `!` marker is special here.** Normally "breaking" means "breaks the
public API for code that links against this." Nobody links against
`kube-shim` as a library — what matters to anyone depending on it is
whether an **already-applied Terraform config or a running deployment**
keeps working. So on this project, `!` means: this PR changes the
Kubernetes API surface (a resource field, an admission rule, a response
shape) or `config.toml`'s schema in a way that breaks an existing
Terraform config or a currently-deployed instance — not that some internal
function signature changed. A `refactor!:` that happens to rename a
`config.toml` field is exactly as `!` as a `feat!:` that removes one; a
`feat:` that adds a new optional field isn't `!` at all. See
[`RELEASING.md`](https://github.com/brawer/kube-shim/blob/main/docs/RELEASING.md#choosing-the-version-number)
for exactly how this maps to version numbers.

## Where to go next

- [`docs/IMPLEMENTATION_PLAN.md`](https://github.com/brawer/kube-shim/blob/main/docs/IMPLEMENTATION_PLAN.md)
  — the phased implementation plan; what's built, what's next, and the
  reasoning behind the design decisions made so far.
- [`docs/QUICKSTART.md`](https://github.com/brawer/kube-shim/blob/main/docs/QUICKSTART.md)
  — running the shim locally.
- [`docs/RELEASING.md`](https://github.com/brawer/kube-shim/blob/main/docs/RELEASING.md)
  — how a release actually ships, for once your change has landed on
  `main`.

That's it — open a PR, and we'll take it from there. 🙂
